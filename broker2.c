#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>

#define BUFFER_SIZE 1024
#define MAX_TOPICS 10
#define MAX_SUBSCRIBERS 10

typedef struct {
    char topic[50];
    int subscribers[MAX_SUBSCRIBERS];
    int sub_count;
} Topic;

Topic topics[MAX_TOPICS];
int topic_count = 0;

pthread_mutex_t lock;

// Remove a subscriber socket from all topics
void remove_subscriber(int sock) {
    pthread_mutex_lock(&lock);

    for (int i = 0; i < topic_count; i++) {
        int index = -1;
        for (int j = 0; j < topics[i].sub_count; j++) {
            if (topics[i].subscribers[j] == sock) {
                index = j;
                break;
            }
        }

        if (index != -1) {
            // Shift remaining subscribers to fill the gap
            for (int j = index; j < topics[i].sub_count - 1; j++) {
                topics[i].subscribers[j] = topics[i].subscribers[j + 1];
            }
            topics[i].sub_count--;
        }
    }

    pthread_mutex_unlock(&lock);
}

// Add a new subscription for a subscriber
void add_subscription(int sock, const char *topic_name) {
    pthread_mutex_lock(&lock);

    int topic_found = 0;
    for (int i = 0; i < topic_count; i++) {
        if (strcmp(topics[i].topic, topic_name) == 0) {
            topic_found = 1;

            // Check if the subscriber is already added
            int already_subscribed = 0;
            for (int j = 0; j < topics[i].sub_count; j++) {
                if (topics[i].subscribers[j] == sock) {
                    already_subscribed = 1;
                    break;
                }
            }

            if (!already_subscribed) {
                topics[i].subscribers[topics[i].sub_count++] = sock;
            }
            break;
        }
    }

    // If topic not found, create it
    if (!topic_found && topic_count < MAX_TOPICS) {
        strcpy(topics[topic_count].topic, topic_name);
        topics[topic_count].subscribers[0] = sock;
        topics[topic_count].sub_count = 1;
        topic_count++;
    }

    pthread_mutex_unlock(&lock);
}

// Function to handle client communication
void *handle_client(void *client_sock) {
    int sock = *(int *)client_sock;
    free(client_sock);

    char buffer[BUFFER_SIZE];
    memset(buffer, 0, BUFFER_SIZE);

    while (1) {
        int bytes_received = recv(sock, buffer, BUFFER_SIZE, 0);
        if (bytes_received <= 0) {
            printf("Subscriber disconnected (socket: %d)\n", sock);
            remove_subscriber(sock);
            close(sock);
            pthread_exit(NULL);
        }

        char *command = strtok(buffer, " ");
        if (strcmp(command, "PUBLISH") == 0) {
            char *topic_name = strtok(NULL, " ");
            char *message = strtok(NULL, "\n");

            pthread_mutex_lock(&lock);

            // Match topic and deliver message only to its subscribers
            for (int i = 0; i < topic_count; i++) {
                if (strcmp(topics[i].topic, topic_name) == 0) {
                    for (int j = 0; j < topics[i].sub_count; j++) {
                        int sub_sock = topics[i].subscribers[j];
                        send(sub_sock, message, strlen(message), 0);
                    }
                    break;
                }
            }

            pthread_mutex_unlock(&lock);
        } else if (strcmp(command, "SUBSCRIBE") == 0) {
            char *topic_name = strtok(NULL, "\n");
            add_subscription(sock, topic_name);
        }

        memset(buffer, 0, BUFFER_SIZE);
    }

    close(sock);
    pthread_exit(NULL);
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <port>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int port = atoi(argv[1]);
    int server_fd, new_socket;
    struct sockaddr_in address;
    int addrlen = sizeof(address);

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 3) < 0) {
        perror("listen failed");
        exit(EXIT_FAILURE);
    }

    pthread_mutex_init(&lock, NULL);

    printf("Broker is running on port %d...\n", port);

    while (1) {
        if ((new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t *)&addrlen)) < 0) {
            perror("accept failed");
            exit(EXIT_FAILURE);
        }

        int *client_sock = malloc(sizeof(int));
        *client_sock = new_socket;

        pthread_t tid;
        pthread_create(&tid, NULL, handle_client, client_sock);
        pthread_detach(tid);
    }

    pthread_mutex_destroy(&lock);
    close(server_fd);
    return 0;
}
