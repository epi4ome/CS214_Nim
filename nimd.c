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
#define MAX_MSG_BYTES 104
#define MAX_NAME_LEN 72
#define INITIAL_CAP 10

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

static void logf(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vprintf(fmt, ap);
    printf("\n");
    va_end(ap);
}

void handler(int signum) {
    (void)signum;
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

/*parser and message builders*/
typedef enum {
    MSG_OPEN, MSG_WAIT, MSG_NAME, MSG_PLAY, MSG_MOVE, MSG_OVER, MSG_FAIL, MSG_INVALID
} msg_type_t;

typedef struct {
    msg_type_t type;
    int field_count;
    char **fields;
} message_t;

static msg_type_t decode_type(const char *t) {
    if (!strncmp(t, "OPEN", 4)) return MSG_OPEN;
    if (!strncmp(t, "WAIT", 4)) return MSG_WAIT;
    if (!strncmp(t, "NAME", 4)) return MSG_NAME;
    if (!strncmp(t, "PLAY", 4)) return MSG_PLAY;
    if (!strncmp(t, "MOVE", 4)) return MSG_MOVE;
    if (!strncmp(t, "OVER", 4)) return MSG_OVER;
    if (!strncmp(t, "FAIL", 4)) return MSG_FAIL;
    return MSG_INVALID;
}

static ssize_t send_ngp(int fd, const char *payload) {
    int len = (int)strlen(payload);
    if (len > MAX_MSG_BYTES) {
        return -1;
    }
    char header[8];
    int hlen = snprintf(header, sizeof(header), "0|%02d|", len);
    if (hlen < 0) {
        return -1;
    }

    int total = hlen + len;
    char *buf = malloc((ssize_t)total + 1);
    if (!buf) {
        return -1;
    }
    
    memcpy(buf, header, hlen);
    memcpy(buf + hlen, payload, len);
    buf[total] = '\0';
    ssize_t w = write(fd, buf, total);
    free(buf);
    return w;
}

static void send_fail(int fd, int code, const char *msg) {
    char payload[256];
    snprintf(payload, sizeof(payload), "FAIL|%d|%s", code, msg);
    send_ngp(fd, payload);
}

static void send_wait(int fd) {
    send_ngp(fd, "WAIT|");
}

static void send_name(int fd, int role, const char *name) {
    char payload[256];
    snprintf(payload, sizeof(payload), "NAME|%d|%s", role, name);
    send_ngp(fd, payload);
}

static void send_play(int fd, int next_role, int board[5]) {
    char b[64];
    snprintf(b, sizeof(b), "%d,%d,%d,%d,%d", board[0], board[1], board[2], board[3], board[4]);
    char payload[256];
    snprintf(payload, sizeof(payload), "PLAY|%d|%s|", next_role, b);
    send_ngp(fd, payload);
}

static void send_over(int fd, int winner, int board[5], int forfeited) {
    char b[64];
    snprintf(b, sizeof(b), "%d,%d,%d,%d,%d", board[0], board[1], board[2], board[3], board[4]);
    char payload[256];
    if (forfeited) {
        snprintf(payload, sizeof(payload), "OVER|%d|%s|Forfeit|", winner, b);
    } 
    else {
        snprintf(payload, sizeof(payload), "OVER|%d|%s||", winner, b);
    }
    send_ngp(fd, payload);
}

static void free_message(message_t *msg) {
    if (!msg) return;
    for (int i = 0; i < msg->field_count; i++) {
        free(msg->fields[i]);
    }
    free(msg->fields);
    m->fields = NULL;
    m->field_count = 0;
    free(msg);
}

static int read_ngp_message_blocking(int fd, message_t *out) {
    out->field_count = 0;
    out->fields = NULL;
    out->type = MSG_INVALID;

    char header[6];
    ssize_t r;
    size_t got = 0;
    while (got < 5) {
        r = recv(fd, header + got, 5 -  got, 0);
        if (r <= 0) {
            return -1;
        }
        got += (size_t)r;
    }
    header[5] = '\0';
    if (!isdigit((unsigned char)header[0]) || header[1] != '|' || !isdigit((unsigned char)header[2]) || !isdigit((unsigned char)header[3]) || header[4] != '|') {
        return 10;
    }
    if (header[0] != '0') {
        return 10;
    }
    int stated_len = (header[2] - '0') * 10 + (header[3] - '0');
    if (stated_len < 1 || stated_len > MAX_MSG_BYTES) {
        return 10;
    }

    char *payload = malloc(stated_len + 1);
    if (!payload) {
        return -1;
    }
    size_t off = 0;
    while (off < (size_t)stated_len) {
        r = recv(fd, payload + off, (size_t)stated_len - off, 0);
        if (r <= 0) {
            free(payload);
            return -1;
        }
        off += (size_t)r;
    }
    payload[stated_len] = '\0';
    if (payload[stated_len - 1] != '|') {
        free(payload);
        return 10;
    }

    /*tokenize*/
    char *save = strdup(payload);
    free(payload);
    if (!save) {
        return -1;
    }
    char *token;
    char *rest = save;
    char *tokens[32];
    int count = 0;
    while ((token = strtok_r(rest, "|", &rest)) != NULL) {
        if (count < 32) {
            tokens[count++] = token;
        }
    }
    if (count < 1) {
        free(save);
        return 10;
    }
    out->type = decode_type(tokens[0]);
    if (out->type == MSG_INVALID) {
        free(save);
        return 10;
    }
    out->field_count = count - 1;
    if (out->field_count > 0) {
        out->fields = calloc(out->field_count, sizeof(char*));
        if (!out->fields) {
            free(save);
            return -1;
        }
        for (int i = 0; i < out->field_count; i++) {
            out->fields[i] = strdup(tokens[i + 1]);
        }
    }
    else{
        out->fields = NULL;
    }
    free(save);
    return 0;
}

/*nonblocking read for use after select*/
int read_ngp_message_nonblocking(int fd, message_t *out) {
    out->field_count = 0;
    out->fields = NULL;
    out->type = MSG_INVALID;

    char header[6] = {0};
    ssize_t r = recv(fd, header, 5, MSG_);
    if (r == 0 ) {
        return -1;
    }
    if (r < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return -2;
        }
        return -1;
    }
    if (r < 5) {
        return -2;
    }

    if (!isdigit((unsigned char)header[0]) || header[1] != '|' || !isdigit((unsigned char)header[2]) || !isdigit((unsigned char)header[3]) || header[4] != '|') {
        return 10;
    }
    if (header[0] != '0') {
        return 10;
    int stated_len = (header[2] - '0') * 10 + (header[3] - '0');
    if (stated_len < 1 || stated_len > MAX_MSG_BYTES) {
        return 10;
    }

    size_t total = (size_t)stated_len + 5;
    char *buf = malloc(total + 1);
    if (!buf) {
        return -1;
    }
    size_t off = 0;
    while (off < total) {
        r = recv(fd, buf + off, total - off, 0);
        if (r == 0) {
            free(buf);
            return -1;
        }
        if (r < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                free(buf);
                return -2;
            }
            free(buf);
            return -1;
        }
        off += (size_t)r;
    }
    buf[total] = '\0';
    char *payload = malloc(stated_len + 1);
    if (!payload) {
        free(buf);
        return -1;
    }
    memcpy(payload, buf + 5, stated_len);
    payload[stated_len] = '\0';
    free(buf);
    if (payload[stated_len - 1] != '|') {
        free(payload);
        return 10;
    }
    char *save = strdup(payload);
    free(payload);
    if (!save) {
        return -1;
    }
    char *token;
    char *rest = save;
    char *tokens[32];
    int count = 0;
    while ((token = strtok_r(rest, "|", &rest)) != NULL) {
        if (count < 32) {
            tokens[count++] = token;
        }
    }
    if (count < 1) {
        free(save);
        return 10;
    }
    out->type = decode_type(tokens[0]);
    if (out->type == MSG_INVALID) {
        free(save);
        return 10;
    }
    out->field_count = count - 1;
    if (out->field_count > 0) {
        out->fields = calloc(out->field_count, sizeof(char*));
        if (!out->fields) {
            free(save);
            return -1;
        }
        for (int i = 0; i < out->field_count; i++) {
            out->fields[i] = strdup(tokens[i + 1]);
        }
    }
    else{
        out->fields = NULL;
    }
    free(save);
    return 0;
}

static int validate_name_field(const char *name) {
    if (!name) return 10;
    size_t len = strlen(name);
    if (len == 0) return 10;
    if (len > MAX_NAME_LEN) return 21;
    for (size_t i = 0; i < len; ++i) {
        unsigned char c = (unsigned char)name[i];
        if (c < 32 || c > 126) return 10;
        if (c == '|') return 10;
    }
    return 0;
}

static int parse_move_fields(Message *m, int *pile, int *qty) {
    if (m->field_count != 2) return 10;
    char *end;
    long p = strtol(m->fields[0], &end, 10);
    if (*end != '\0') return 10;
    long q = strtol(m->fields[1], &end, 10);
    if (*end != '\0') return 10;
    *pile = (int)p;
    *qty = (int)q;
    return 0;
}
 
//just need to alter this below to include extra credit and implemented game logic
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
