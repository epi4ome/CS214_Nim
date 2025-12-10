#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#define BUF_SIZE 1024

void build_msg(char *out, const char *type, const char *fields) {
    char body[300];
    snprintf(body, sizeof(body), "%s|%s|", type, fields);
    int len = strlen(body);
    snprintf(out, 400, "0|%02d|%s", len, body);
}

void send_raw(int sock, const char *raw_msg) {
    send(sock, raw_msg, strlen(raw_msg), 0);
    printf(">>> SENT RAW: %s\n", raw_msg);
}

void send_open(int sock, const char *name) {
    char msg[400];
    build_msg(msg, "OPEN", name);
    send(sock, msg, strlen(msg), 0);
    printf(">>> SENT: %s\n", msg);
}

void send_move(int sock, int pile, int qty) {
    char field[50], msg[200];
    snprintf(field, sizeof(field), "%d|%d", pile, qty);
    build_msg(msg, "MOVE", field);
    send(sock, msg, strlen(msg), 0);
    printf(">>> SENT MOVE: %s\n", msg);
}

int main(int argc, char *argv[]) {
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <ip> <port> <player-name> [test-mode]\n", argv[0]);
        exit(1);
    }

    char *ip = argv[1];
    int port = atoi(argv[2]);
    char *name = argv[3];
    int mode = (argc >= 5) ? atoi(argv[4]) : 0;

    int myPlayerNum = 0;

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in serv;
    serv.sin_family = AF_INET;
    serv.sin_port = htons(port);
    inet_pton(AF_INET, ip, &serv.sin_addr);

    if (connect(sock, (struct sockaddr *)&serv, sizeof(serv)) < 0) {
        perror("connect");
        exit(1);
    }
    printf("Connected to nimd (Mode: %d).\n", mode);

    if (mode == 7) {
        send_raw(sock, "0|21|GARBAGE_DATA_NO_PIPES");
        char buf[1024];
        int n = recv(sock, buf, 1023, 0);
        if (n > 0) { buf[n] = 0; printf("<<< RECV: %s\n", buf); }
        else printf("<<< RECV: Connection closed (EOF).\n");
        close(sock); return 0;
    }

    if (mode == 8) {
        send_raw(sock, "0|06|JUNK||");
        char buf[1024];
        int n = recv(sock, buf, 1023, 0);
        if (n > 0) { buf[n] = 0; printf("<<< RECV: %s\n", buf); }
        else printf("<<< RECV: Connection closed (EOF).\n");
        close(sock); return 0;
    }

    if (mode == 1) {
        char longName[100];
        memset(longName, 'A', 80);
        longName[80] = '\0';
        send_open(sock, longName);
    } else {
        send_open(sock, name);
    }

    if (mode == 3) {
        usleep(50000);
        send_move(sock, 0, 1);
    }

    if (mode == 2) {
        usleep(100000);
        send_open(sock, name);
    }

    char buf[BUF_SIZE];
    int n;
    while ((n = recv(sock, buf, BUF_SIZE - 1, 0)) > 0) {
        buf[n] = '\0';
        printf("<<< RECV: %s\n", buf);

        char *ptr = buf;
        while (1) {
            char *p1 = strchr(ptr, '|');
            if (!p1) break;
            char *p2 = strchr(p1 + 1, '|');
            if (!p2) break;

            int msgLen = 0;
            if (sscanf(ptr + 2, "%d", &msgLen) != 1) break;

            char *typePtr = p2 + 1;
            char type[5];
            strncpy(type, typePtr, 4);
            type[4] = '\0';

            if (strcmp(type, "WAIT") == 0) {
                // no-op
            }
            else if (strcmp(type, "NAME") == 0) {
                myPlayerNum = atoi(typePtr + 5);
                printf("Game Start. I am player %d.\n", myPlayerNum);
                sleep(30);

                if (mode == 9) {
                    printf(">>> Mode 9: Intentionally disconnecting to trigger FORFEIT.\n");
                    close(sock);
                    exit(0);
                }
            }
            else if (strcmp(type, "PLAY") == 0) {
                int turn = atoi(typePtr + 5);

                if (mode == 4 && turn != myPlayerNum) {
                    printf("Testing Impatient: Moving out of turn...\n");
                    send_move(sock, 0, 1);
                }

                if (turn == myPlayerNum) {
                    char *boardStr = strchr(typePtr + 5, '|') + 1;
                    int piles[5];
                    sscanf(boardStr, "%d %d %d %d %d", &piles[0], &piles[1], &piles[2], &piles[3], &piles[4]);

                    if (mode == 10) { usleep(500000); }

                    if (mode == 5) { send_move(sock, 9, 1); }
                    else if (mode == 6) { send_move(sock, 0, 100); }
                    else {
                        int pileToMove = -1;
                        for (int i = 0; i < 5; i++)
                            if (piles[i] > 0) { pileToMove = i; break; }
                        if (pileToMove != -1) send_move(sock, pileToMove, 1);
                    }
                }
            }
            else if (strcmp(type, "OVER") == 0) {
                printf("Game Over.\n");
                goto end_loop;
            }
            else if (strcmp(type, "FAIL") == 0) {
                printf("FAIL received.\n");
                goto end_loop;
            }

            ptr = p2 + 1 + msgLen;
            if (ptr >= buf + n) break;
        }
    }

end_loop:
    close(sock);
    return 0;
}
