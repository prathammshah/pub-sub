 gcc broker.c -o broker
 gcc publisher.c -o publisher
 gcc subscriber.c -o subscriber

 ./broker 8080 cricket
 ./broker 8081 food
 ./broker 8082 movies

 ./publisher cricket:127.0.0.1:8080 food:127.0.0.1:8081 movies:127.0.0.1:8082

 ./subscriber cricket:127.0.0.1:8080 food:127.0.0.1:8081 movies:127.0.0.1:8082



commands:
gcc broker.c -o broker -lpthread
./broker 8080
./subscriber 127.0.0.1 8080
./publisher 127.0.0.1 8080

Path to Desktop:
/mnt/c/Users/'Gurujeet Singh'/OneDrive/Desktop