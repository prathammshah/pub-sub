#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <netdb.h>

#define BUFFER_SIZE 1024
#define MAX_TOPICS 10
#define MAX_SUBSCRIBERS 10
#define MAX_BROKERS 5

typedef struct {
    char topic[50];
    int subscribers[MAX_SUBSCRIBERS];
    int sub_count;
} Topic;

typedef struct {
    char ip[50];
    int port;
} Broker;

Topic topics[MAX_TOPICS];
int topic_count = 0;
Broker brokers[MAX_BROKERS];
int broker_count = 0;
int my_broker_id = 0; // Unique ID for this broker
pthread_mutex_t lock;

// Function to calculate the responsible broker for a topic
int get_broker_for_topic(const char *topic_name) {
    unsigned long hash = 0;
    for (int i = 0; topic_name[i] != '\0'; i++) {
        hash = (hash * 31 + topic_name[i]) % broker_count;
    }
    return hash % broker_count;
}

// Add a new broker to the list
void add_broker(const char *ip, int port) {
    if (broker_count >= MAX_BROKERS) {
        fprintf(stderr, "[ERROR] Maximum number of brokers reached.\n");
        return;
    }
    strncpy(brokers[broker_count].ip, ip, sizeof(brokers[broker_count].ip) - 1);
    brokers[broker_count].port = port;
    printf("[DEBUG] Broker added: %s:%d\n", ip, port);
    broker_count++;
}

// Forward a message to another broker
void forward_message_to_broker(const char *ip, int port, const char *message) {
    int sock;
    struct sockaddr_in broker_address;

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("Socket creation error");
        return;
    }

    broker_address.sin_family = AF_INET;
    broker_address.sin_port = htons(port);
    if (inet_pton(AF_INET, ip, &broker_address.sin_addr) <= 0) {
        perror("Invalid broker IP address");
        close(sock);
        return;
    }

    if (connect(sock, (struct sockaddr *)&broker_address, sizeof(broker_address)) < 0) {
        perror("Connection to broker failed");
        close(sock);
        return;
    }

    send(sock, message, strlen(message), 0);
    close(sock);
}

// Handle client connections
void *handle_client(void *client_sock) {
    int sock = *(int *)client_sock;
    free(client_sock);

    printf("[DEBUG] Handling client connection on socket %d...\n", sock);

    char buffer[BUFFER_SIZE];
    memset(buffer, 0, BUFFER_SIZE);

    while (1) {
        int bytes_received = recv(sock, buffer, BUFFER_SIZE, 0);

        if (bytes_received <= 0) {
            if (bytes_received == 0) {
                printf("[DEBUG] Client disconnected (socket %d).\n", sock);
            } else {
                perror("[ERROR] recv failed");
            }
            close(sock);
            pthread_exit(NULL);
        }

        buffer[bytes_received] = '\0';
        printf("[DEBUG] Received: %s\n", buffer);

        char *command = strtok(buffer, " ");
        if (!command) {
            fprintf(stderr, "[ERROR] Invalid command received.\n");
            continue;
        }

        if (strcmp(command, "PUBLISH") == 0) {
            char *topic_name = strtok(NULL, " ");
            char *message = strtok(NULL, "\n");

            if (!topic_name || !message) {
                fprintf(stderr, "[ERROR] Invalid PUBLISH format.\n");
                continue;
            }

            int broker_id = get_broker_for_topic(topic_name);
            if (broker_id != my_broker_id) {
                char forward_msg[BUFFER_SIZE];
                snprintf(forward_msg, BUFFER_SIZE, "FORWARD PUBLISH %s %s", topic_name, message);
                forward_message_to_broker(brokers[broker_id].ip, brokers[broker_id].port, forward_msg);
            } else {
                pthread_mutex_lock(&lock);
                for (int i = 0; i < topic_count; i++) {
                    if (strcmp(topics[i].topic, topic_name) == 0) {
                        for (int j = 0; j < topics[i].sub_count; j++) {
                            send(topics[i].subscribers[j], message, strlen(message), 0);
                        }
                        break;
                    }
                }
                pthread_mutex_unlock(&lock);
            }

        } else if (strcmp(command, "SUBSCRIBE") == 0) {
            char *topic_name = strtok(NULL, "\n");
            if (!topic_name) {
                fprintf(stderr, "[ERROR] Invalid SUBSCRIBE format.\n");
                continue;
            }

            int broker_id = get_broker_for_topic(topic_name);
            if (broker_id != my_broker_id) {
                char forward_msg[BUFFER_SIZE];
                snprintf(forward_msg, BUFFER_SIZE, "FORWARD SUBSCRIBE %s", topic_name);
                forward_message_to_broker(brokers[broker_id].ip, brokers[broker_id].port, forward_msg);
            } else {
                pthread_mutex_lock(&lock);
                int topic_found = 0;
                for (int i = 0; i < topic_count; i++) {
                    if (strcmp(topics[i].topic, topic_name) == 0) {
                        topics[i].subscribers[topics[i].sub_count++] = sock;
                        topic_found = 1;
                        break;
                    }
                }
                if (!topic_found && topic_count < MAX_TOPICS) {
                    strncpy(topics[topic_count].topic, topic_name, sizeof(topics[topic_count].topic) - 1);
                    topics[topic_count].subscribers[0] = sock;
                    topics[topic_count].sub_count = 1;
                    topic_count++;
                }
                pthread_mutex_unlock(&lock);
            }

        } else if (strcmp(command, "FORWARD") == 0) {
            char *forward_type = strtok(NULL, " ");
            if (strcmp(forward_type, "PUBLISH") == 0) {
                char *topic_name = strtok(NULL, " ");
                char *message = strtok(NULL, "\n");

                if (!topic_name || !message) {
                    fprintf(stderr, "[ERROR] Invalid FORWARD PUBLISH format.\n");
                    continue;
                }

                int broker_id = get_broker_for_topic(topic_name);
                if (broker_id == my_broker_id) {
                    pthread_mutex_lock(&lock);
                    for (int i = 0; i < topic_count; i++) {
                        if (strcmp(topics[i].topic, topic_name) == 0) {
                            for (int j = 0; j < topics[i].sub_count; j++) {
                                send(topics[i].subscribers[j], message, strlen(message), 0);
                            }
                            break;
                        }
                    }
                    pthread_mutex_unlock(&lock);
                }

            } else if (strcmp(forward_type, "SUBSCRIBE") == 0) {
                char *topic_name = strtok(NULL, "\n");

                if (!topic_name) {
                    fprintf(stderr, "[ERROR] Invalid FORWARD SUBSCRIBE format.\n");
                    continue;
                }

                int broker_id = get_broker_for_topic(topic_name);
                if (broker_id == my_broker_id) {
                    pthread_mutex_lock(&lock);
                    int topic_found = 0;
                    for (int i = 0; i < topic_count; i++) {
                        if (strcmp(topics[i].topic, topic_name) == 0) {
                            topics[i].subscribers[topics[i].sub_count++] = sock;
                            topic_found = 1;
                            break;
                        }
                    }
                    if (!topic_found && topic_count < MAX_TOPICS) {
                        strncpy(topics[topic_count].topic, topic_name, sizeof(topics[topic_count].topic) - 1);
                        topics[topic_count].subscribers[0] = sock;
                        topics[topic_count].sub_count = 1;
                        topic_count++;
                    }
                    pthread_mutex_unlock(&lock);
                }
            }

        } else {
            fprintf(stderr, "[ERROR] Unknown command: %s\n", command);
        }
    }

    close(sock);
    pthread_exit(NULL);
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <port> <peer_ip:peer_port>...\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int port = atoi(argv[1]);
    for (int i = 2; i < argc; i++) {
        char *colon = strchr(argv[i], ':');
        if (colon) {
            *colon = '\0';
            add_broker(argv[i], atoi(colon + 1));
        }
    }

    printf("[DEBUG] Broker running on port %d...\n", port);

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    bind(server_fd, (struct sockaddr *)&address, sizeof(address));
    listen(server_fd, 3);

    pthread_mutex_init(&lock, NULL);

    while (1) {
        int new_socket = accept(server_fd, NULL, NULL);
        pthread_t thread;
        int *client_sock = malloc(sizeof(int));
        *client_sock = new_socket;
        pthread_create(&thread, NULL, handle_client, client_sock);
        pthread_detach(thread);
    }

    pthread_mutex_destroy(&lock);
    close(server_fd);
    return 0;
}
