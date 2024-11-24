#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>

#define BUFFER_SIZE 1024
#define MAX_TOPICS 10
#define MAX_CONNECTIONS 10

typedef struct {
    char topic[50];
    char ip[50];
    int port;
} TopicBroker;

typedef struct {
    int sock;
    char topic[50];
} Connection;

TopicBroker topic_brokers[MAX_TOPICS];
int topic_count = 0;

Connection connections[MAX_CONNECTIONS];
int connection_count = 0;

void add_topic_broker(const char *topic, const char *ip, int port) {
    if (topic_count >= MAX_TOPICS) {
        fprintf(stderr, "[ERROR] Maximum topics reached.\n");
        return;
    }
    strcpy(topic_brokers[topic_count].topic, topic);
    strcpy(topic_brokers[topic_count].ip, ip);
    topic_brokers[topic_count].port = port;
    topic_count++;
}

int connect_to_broker(const char *topic) {
    for (int i = 0; i < topic_count; i++) {
        if (strcmp(topic_brokers[i].topic, topic) == 0) {
            int sock = socket(AF_INET, SOCK_STREAM, 0);
            struct sockaddr_in broker_address;
            broker_address.sin_family = AF_INET;
            broker_address.sin_port = htons(topic_brokers[i].port);
            inet_pton(AF_INET, topic_brokers[i].ip, &broker_address.sin_addr);

            if (connect(sock, (struct sockaddr *)&broker_address, sizeof(broker_address)) < 0) {
                perror("[ERROR] Connection to broker failed");
                return -1;
            }

            return sock;
        }
    }
    fprintf(stderr, "[ERROR] No broker found for topic '%s'.\n", topic);
    return -1;
}

void *listen_to_broker(void *arg) {
    Connection *conn = (Connection *)arg;
    char buffer[BUFFER_SIZE];

    // printf("[DEBUG] Listening to messages for topic '%s'...\n", conn->topic);  

    while (1) {
        memset(buffer, 0, BUFFER_SIZE);
        int bytes_received = recv(conn->sock, buffer, BUFFER_SIZE, 0);
        if (bytes_received > 0) {
            printf("Message received on topic '%s': %s\n", conn->topic, buffer);
        } else if (bytes_received == 0) {
            printf("[DEBUG] Broker closed connection for topic '%s'.\n", conn->topic);
            break;
        } else {
            perror("[ERROR] recv failed");
            break;
        }
    }

    close(conn->sock);
    pthread_exit(NULL);
}

void subscribe_to_topics() {
    char topic[BUFFER_SIZE];

    while (1) {
        printf("\nEnter topic to subscribe (or 'exit' to quit): ");
        fgets(topic, sizeof(topic), stdin);
        topic[strcspn(topic, "\n")] = '\0';

        if (strcmp(topic, "exit") == 0) {
            break;
        }

        int sock = connect_to_broker(topic);
        if (sock < 0) continue;

        char buffer[BUFFER_SIZE];
        snprintf(buffer, sizeof(buffer), "SUBSCRIBE %s", topic);
        send(sock, buffer, strlen(buffer), 0);

        printf("[DEBUG] Subscribed to topic '%s'.\n", topic);

        // Store connection
        if (connection_count < MAX_CONNECTIONS) {
            connections[connection_count].sock = sock;
            strcpy(connections[connection_count].topic, topic);

            pthread_t thread;
            pthread_create(&thread, NULL, listen_to_broker, &connections[connection_count]);
            pthread_detach(thread);

            connection_count++;
        } else {
            printf("[ERROR] Maximum connections reached.\n");
            close(sock);
        }
    }
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <topic:ip:port>...\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    for (int i = 1; i < argc; i++) {
        char topic[50], ip[50];
        int port;
        sscanf(argv[i], "%[^:]:%[^:]:%d", topic, ip, &port);
        add_topic_broker(topic, ip, port);
    }

    subscribe_to_topics();
    return 0;
}
