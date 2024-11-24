#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define BUFFER_SIZE 1024
#define MAX_BROKERS 5

typedef struct {
    char ip[50];
    int port;
} Broker;

Broker brokers[MAX_BROKERS];
int broker_count = 0;

// Function to calculate the responsible broker for a topic
int get_broker_for_topic(const char *topic_name) {
    unsigned long hash = 0;
    for (int i = 0; topic_name[i] != '\0'; i++) {
        hash = (hash * 31 + topic_name[i]) % broker_count;
    }
    return hash % broker_count;
}

// Add a broker to the list
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

int connect_to_broker(int broker_id) {
    int sock;
    struct sockaddr_in broker_address;

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("[ERROR] Socket creation error");
        return -1;
    }

    broker_address.sin_family = AF_INET;
    broker_address.sin_port = htons(brokers[broker_id].port);
    if (inet_pton(AF_INET, brokers[broker_id].ip, &broker_address.sin_addr) <= 0) {
        perror("[ERROR] Invalid broker IP address");
        close(sock);
        return -1;
    }

    if (connect(sock, (struct sockaddr *)&broker_address, sizeof(broker_address)) < 0) {
        fprintf(stderr, "[ERROR] Connection failed to broker %s:%d\n", brokers[broker_id].ip, brokers[broker_id].port);
        close(sock);
        return -1;
    }

    return sock;
}

void publish_messages() {
    char topic[BUFFER_SIZE];
    char message[BUFFER_SIZE];
    char buffer[BUFFER_SIZE];

    while (1) {
        printf("\nEnter topic to publish (or 'exit' to quit): ");
        fgets(topic, sizeof(topic), stdin);
        topic[strcspn(topic, "\n")] = '\0'; // Remove newline character

        if (strcmp(topic, "exit") == 0) {
            break;
        }

        printf("Enter message: ");
        fgets(message, sizeof(message), stdin);
        message[strcspn(message, "\n")] = '\0'; // Remove newline character

        // Calculate required buffer size
        size_t required_size = strlen("PUBLISH ") + strlen(topic) + strlen(message) + 1;

        if (required_size > sizeof(buffer)) {
            fprintf(stderr, "[ERROR] Topic and message length exceed allowed size.\n");
            continue;
        }

        int broker_id = get_broker_for_topic(topic);
        int sock = connect_to_broker(broker_id);
        if (sock < 0) continue;

        snprintf(buffer, sizeof(buffer), "PUBLISH %s %s", topic, message);
        send(sock, buffer, strlen(buffer), 0);

        printf("[DEBUG] Published: Topic='%s', Message='%s' (via Broker %d)\n", topic, message, broker_id);

        close(sock);
    }
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <broker_ip:port>...\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    for (int i = 1; i < argc; i++) {
        char *colon = strchr(argv[i], ':');
        if (colon) {
            *colon = '\0';
            add_broker(argv[i], atoi(colon + 1));
        }
    }

    printf("[DEBUG] Publisher started. Type 'exit' to quit.\n");
    publish_messages();

    return 0;
}