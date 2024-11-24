#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define BUFFER_SIZE 1024

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <broker_ip> <port>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    char *broker_ip = argv[1];
    int port = atoi(argv[2]);

    int sock;
    struct sockaddr_in server_address;
    char buffer[BUFFER_SIZE];
    char topic[50];

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("Socket creation error");
        return -1;
    }

    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(port);
    if (inet_pton(AF_INET, broker_ip, &server_address.sin_addr) <= 0) {
        perror("Invalid address");
        return -1;
    }

    if (connect(sock, (struct sockaddr *)&server_address, sizeof(server_address)) < 0) {
        perror("Connection failed");
        return -1;
    }

    printf("Connected to broker. Type 'exit' to stop subscribing.\n");

    while (1) {
        printf("\nEnter topic to subscribe (or 'exit' to quit): ");
        fgets(topic, sizeof(topic), stdin);
        topic[strcspn(topic, "\n")] = 0;

        if (strcmp(topic, "exit") == 0) {
            printf("Stopped subscribing.\n");
            break;
        }

        snprintf(buffer, BUFFER_SIZE, "SUBSCRIBE %s", topic);
        if (send(sock, buffer, strlen(buffer), 0) < 0) {
            perror("Send failed");
            close(sock);
            return -1;
        }

        printf("Subscribed to topic '%s'.\n", topic);
    }

    printf("Listening for messages...\n");
    while (1) {
        memset(buffer, 0, BUFFER_SIZE);
        int bytes_received = recv(sock, buffer, BUFFER_SIZE, 0);
        if (bytes_received > 0) {
            printf("Message received: %s\n", buffer);
        } else if (bytes_received == 0) {
            printf("Connection closed by broker.\n");
            break;
        } else {
            perror("recv failed");
            break;
        }
    }

    close(sock);
    return 0;
}
