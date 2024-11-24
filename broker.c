#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>

#define BUFFER_SIZE 1024
#define MAX_SUBSCRIBERS 10

typedef struct {
    int subscribers[MAX_SUBSCRIBERS];
    int sub_count;
    pthread_mutex_t lock;
} Broker;

Broker broker;
char assigned_topic[50];

void handle_client(int client_sock) {
    char buffer[BUFFER_SIZE];
    memset(buffer, 0, BUFFER_SIZE);

    while (1) {
        int bytes_received = recv(client_sock, buffer, BUFFER_SIZE, 0);
        if (bytes_received <= 0) {
            close(client_sock);
            pthread_exit(NULL);
        }

        char *command = strtok(buffer, " ");
        if (strcmp(command, "PUBLISH") == 0) {
            char *topic = strtok(NULL, " ");
            char *message = strtok(NULL, "\n");

            if (strcmp(topic, assigned_topic) != 0) {
                fprintf(stderr, "[ERROR] Invalid topic '%s' for this broker.\n", topic);
                continue;
            }

            pthread_mutex_lock(&broker.lock);
            for (int i = 0; i < broker.sub_count; i++) {
                send(broker.subscribers[i], message, strlen(message), 0);
            }
            pthread_mutex_unlock(&broker.lock);

            printf("[DEBUG] Message '%s' published to topic '%s'.\n", message, topic);
        } else if (strcmp(command, "SUBSCRIBE") == 0) {
            pthread_mutex_lock(&broker.lock);
            broker.subscribers[broker.sub_count++] = client_sock;
            pthread_mutex_unlock(&broker.lock);
            printf("[DEBUG] Client subscribed to topic '%s'.\n", assigned_topic);
        } else {
            fprintf(stderr, "[ERROR] Unknown command '%s'.\n", command);
        }

        memset(buffer, 0, BUFFER_SIZE);
    }
}

void *client_handler(void *arg) {
    int client_sock = *(int *)arg;
    free(arg);
    handle_client(client_sock);
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <port> <topic>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int port = atoi(argv[1]);
    strcpy(assigned_topic, argv[2]);
    pthread_mutex_init(&broker.lock, NULL);

    int server_sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    bind(server_sock, (struct sockaddr *)&address, sizeof(address));
    listen(server_sock, 3);

    printf("[DEBUG] Broker for topic '%s' running on port %d...\n", assigned_topic, port);

    while (1) {
        int *client_sock = malloc(sizeof(int));
        *client_sock = accept(server_sock, NULL, NULL);
        pthread_t thread;
        pthread_create(&thread, NULL, client_handler, client_sock);
        pthread_detach(thread);
    }

    pthread_mutex_destroy(&broker.lock);
    close(server_sock);
    return 0;
}
