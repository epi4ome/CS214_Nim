#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netdb.h>
#include <signal.h>

#define QUEUE 10

volatile int active = 1;

typedef struct {
    int sock;
    char *name;
} client_t;

typedef struct {
    client_t *player1;
    client_t *player2;
} game_t;

client_t **wait_clients = NULL;
int wait_count = 0;
int wait_cap = 0;

client_t **act_clients = NULL;
int act_count = 0;
int act_cap = 0;

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

void handler(int signum) {
    active = 0;
}

int open_listener(const char *service, int queue_size) {
    struct addrinfo hints, *res, *p;
    int listener_sock = -1;
    int opt = 1;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    if (getaddrinfo(NULL, service, &hints, &res) != 0) {
        perror("getaddrinfo failed");
        return -1;
    }

    for (p = res; p != NULL; p = p->ai_next) {
        listener_sock = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (listener_sock < 0) continue;
        if (setsockopt(listener_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
            perror("setsockopt SO_REUSEADDR failed");
            close(listener_sock);
            continue;
        }
        if (bind(listener_sock, p->ai_addr, p->ai_addrlen) < 0) {
            close(listener_sock);
            continue;
        }
        break;
    }

    freeaddrinfo(res);

    if (p == NULL) {
        fprintf(stderr, "Server failed to bind\n");
        return -1;
    }
    if (listen(listener_sock, queue_size) < 0) {
        perror("listen failed");
        close(listener_sock);
        return -1;
    }
    return listener_sock;
}

void install_handlers(void) {
    struct sigaction act;
    act.sa_handler = handler;
    act.sa_flags = 0;
    sigemptyset(&act.sa_mask);
    sigaddset(&act.sa_mask, SIGINT);
    sigaddset(&act.sa_mask, SIGTERM);
    sigaction(SIGINT, &act, NULL);
    sigaction(SIGTERM, &act, NULL);
}

void remove_act(client_t *c) {
    pthread_mutex_lock(&mutex);
    int i;
    for (i = 0; i < act_count; i++) {
        if (act_clients[i] == c) {
            act_clients[i] = act_clients[act_count - 1];
            act_count--;
            break;
        }
    }
    pthread_mutex_unlock(&mutex);
}

void *game_thread(void *arg) {
    game_t *game = (game_t *)arg;
    client_t *p1 = game->player1;
    client_t *p2 = game->player2;
    int turns = 0;

    write(p1->sock, "MATCHED\n", 8);
    write(p2->sock, "MATCHED\n", 8);

    while (active && turns < 5) {
        sleep(1);
        turns++;
    }
    close(p1->sock);
    close(p2->sock);
    free(p1->name);
    free(p2->name);
    remove_act(p1);
    remove_act(p2);
    free(p1);
    free(p2);
    free(game);
    return NULL;
}

void add_wait(client_t *c) {
    pthread_mutex_lock(&mutex);
    int i;
    for (i = 0; i < act_count; i++) {
        if (strcmp(act_clients[i]->name, c->name) == 0) {
            fprintf(stderr, "Rejected duplicate name: %s\n", c->name);
            write(c->sock, "ERROR: Name already in use\n", 27);
            close(c->sock);
            free(c->name);
            free(c);
            pthread_mutex_unlock(&mutex);
            return;
        }
    }

    if (act_count == act_cap) {
        if (act_cap == 0) {
            act_cap = 10;
        } else {
            act_cap = act_cap * 2;
        }
        client_t **temp = realloc(act_clients, sizeof(client_t*) * act_cap);
        if (temp == NULL) {
            fprintf(stderr, "realloc for act_clients failed\n");
            close(c->sock);
            free(c->name);
            free(c);
            pthread_mutex_unlock(&mutex);
            return;
        }
        act_clients = temp;
    }
    act_clients[act_count++] = c;

    if (wait_count > 0) {
        client_t *p2 = wait_clients[wait_count - 1];
        wait_count--;

        game_t *game = malloc(sizeof(game_t));
        if (game == NULL) {
            fprintf(stderr, "malloc for game_t failed. Disconnecting clients.\n");
            remove_act(c);
            remove_act(p2);
            close(c->sock);
            close(p2->sock);
            free(c->name);
            free(p2->name);
            free(c);
            free(p2);
            pthread_mutex_unlock(&mutex);
            return;
        }

        game->player1 = c;
        game->player2 = p2;

        pthread_t tid;
        if (pthread_create(&tid, NULL, game_thread, game) != 0) {
            perror("pthread_create failed");
            free(game);
            pthread_mutex_unlock(&mutex);
            return;
        }
        pthread_detach(tid);
    } else {
        if (wait_count == wait_cap) {
            if (wait_cap == 0) {
                wait_cap = 10;
            } else {
                wait_cap = wait_cap * 2;
            }

            client_t **temp = realloc(wait_clients, sizeof(client_t*) * wait_cap);
            if (temp == NULL) {
                fprintf(stderr, "realloc for wait_clients failed\n");
                remove_act(c);
                close(c->sock);
                free(c->name);
                free(c);
                pthread_mutex_unlock(&mutex);
                return;
            }
            wait_clients = temp;
        }
        wait_clients[wait_count++] = c;
        write(c->sock, "WAITING\n", 8);
    }
    pthread_mutex_unlock(&mutex);
}

void handle_client(int sock) {
    char buf[256];
    int bytes = read(sock, buf, sizeof(buf)-1);

    if (bytes <= 0) {
        if (bytes < 0) perror("read client name failed");
        close(sock);
        return;
    }

    if (buf[bytes-1] == '\n' || buf[bytes-1] == '\r') {
        buf[bytes-1] = '\0';
    } else {
        buf[bytes] = '\0';
    }

    client_t *c = malloc(sizeof(client_t));
    if (c == NULL) {
        perror("malloc client_t failed");
        close(sock);
        return;
    }

    c->sock = sock;
    c->name = strdup(buf);
    if (c->name == NULL) {
        perror("strdup failed");
        free(c);
        close(sock);
        return;
    }

    add_wait(c);
}

int main(int argc, char **argv) {
    char *service;
    if (argc >= 2) {
        service = argv[1];
    } else {
        service = "15000";
    }
    install_handlers();

    int listener = open_listener(service, QUEUE);
    if (listener < 0) exit(EXIT_FAILURE);

    printf("Server listening on port %s...\n", service);

    while (active) {
        struct sockaddr_storage remote_host;
        socklen_t remote_host_len = sizeof(remote_host);

        int sock = accept(listener, (struct sockaddr *)&remote_host, &remote_host_len);
        if (sock < 0) {
            if (active) perror("accept failed");
            continue;
        }
        handle_client(sock);
    }

    close(listener);
    free(wait_clients);
    free(act_clients);
    return EXIT_SUCCESS;
}
