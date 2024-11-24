#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define BUFFER_SIZE 1024
#define MAX_TOPICS 10

typedef struct {
    char topic[50];
    char ip[50];
    int port;
} TopicBroker;

TopicBroker topic_brokers[MAX_TOPICS];
int topic_count = 0;

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
            connect(sock, (struct sockaddr *)&broker_address, sizeof(broker_address));
            return sock;
        }
    }
    fprintf(stderr, "[ERROR] No broker found for topic '%s'.\n", topic);
    return -1;
}

void publish_message() {
    char topic[BUFFER_SIZE];
    char message[BUFFER_SIZE];

    while (1) {
        printf("\nEnter topic to publish (or 'exit' to quit): ");
        fgets(topic, sizeof(topic), stdin);
        topic[strcspn(topic, "\n")] = '\0';

        if (strcmp(topic, "exit") == 0) {
            break;
        }

        printf("Enter message: ");
        fgets(message, sizeof(message), stdin);
        message[strcspn(message, "\n")] = '\0';

        int sock = connect_to_broker(topic);
        if (sock < 0) continue;

        char buffer[BUFFER_SIZE];
        snprintf(buffer, sizeof(buffer), "PUBLISH %s %s", topic, message);
        send(sock, buffer, strlen(buffer), 0);
        close(sock);

        printf("[DEBUG] Published '%s' to topic '%s'.\n", message, topic);
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

    publish_message();
    return 0;
}
