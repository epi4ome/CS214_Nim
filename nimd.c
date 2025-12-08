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
#include <sys/time.h>
#include <sys/select.h>
#include <ctype.h>
#include <errno.h>
#include <stdarg.h>
#include <stdint.h>
#include <limits.h>

#define QUEUE 10
#define MAX_MSG_BYTES 104
#define MAX_NAME_LEN 72
#define INITIAL_CAP 10

volatile int active = 1;

typedef enum {
    MSG_OPEN, MSG_WAIT, MSG_NAME, MSG_PLAY, MSG_MOVE, MSG_OVER, MSG_FAIL, MSG_INVALID
} msg_type_t;

typedef struct {
    msg_type_t type;
    int field_count;
    char **fields;
} message_t;

typedef struct {
    int sock;
    char *name;
    int game_id;
    int player_num;
    int is_waiting;
    int is_playing;
} client_t;

typedef struct {
    int id;
    client_t *player1;
    client_t *player2;
    int board[5];
    int current_turn;
    pthread_mutex_t lock;
    int game_over;
    int winner;
    int forfeited;
} game_t;

client_t **clients = NULL;
int client_count = 0;
int client_cap = 0;

game_t **games = NULL;
int game_count = 0;
int game_cap = 0;
int next_game_id = 1;

pthread_mutex_t global_mutex = PTHREAD_MUTEX_INITIALIZER;

static void logf(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vprintf(fmt, ap);
    printf("\n");
    fflush(stdout);
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
    /* avoid process termination when writing to closed sockets */
    signal(SIGPIPE, SIG_IGN);
}

/* removed unused remove_act() - list management uses add_client/remove_client */

/*parser and message builders*/

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
    char *buf = malloc((size_t)total + 1);
    if (!buf) {
        return -1;
    }
    
    memcpy(buf, header, (size_t)hlen);
    memcpy(buf + hlen, payload, (size_t)len);
    ssize_t sent = 0;
    while ((size_t)sent < (size_t)total) {
        ssize_t w = send(fd, buf + sent, (size_t)total - (size_t)sent, 0);
        if (w < 0) {
            if (errno == EINTR) continue;
            free(buf);
            return -1;
        }
        sent += w;
    }
    free(buf);
    return sent;
}

static void send_fail(int fd, int code, const char *msg) {
    char payload[256];
    int n = snprintf(payload, sizeof(payload), "FAIL|%d %s|", code, msg);
    if (n < 0) return;
    if (n > MAX_MSG_BYTES) return; /* don't attempt to send oversized protocol payload */
    send_ngp(fd, payload);
}

static void send_wait(int fd) {
    send_ngp(fd, "WAIT|");
}

static void send_name(int fd, int role, const char *name) {
    char payload[256];
    snprintf(payload, sizeof(payload), "NAME|%d|%s|", role, name);
    send_ngp(fd, payload);
}

static void send_play(int fd, int next_role, int board[5]) {
    char b[64];
    snprintf(b, sizeof(b), "%d %d %d %d %d", board[0], board[1], board[2], board[3], board[4]);
    char payload[256];
    snprintf(payload, sizeof(payload), "PLAY|%d|%s|", next_role, b);
    send_ngp(fd, payload);
}

static void send_over(int fd, int winner, int board[5], int forfeited) {
    char b[64];
    snprintf(b, sizeof(b), "%d %d %d %d %d", board[0], board[1], board[2], board[3], board[4]);
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
    msg->fields = NULL;
    msg->field_count = 0;
}

static int read_ngp_message(int fd, message_t *out) {
    out->field_count = 0;
    out->fields = NULL;
    out->type = MSG_INVALID;

    char header[6];
    ssize_t r;
    size_t got = 0;
    
    while (got < 5) {
        r = recv(fd, header + got, 5 - got, 0);
        if (r <= 0) {
            return -1;
        }
        got += (size_t)r;
    }
    
    if (!isdigit((unsigned char)header[0]) || header[1] != '|' || 
        !isdigit((unsigned char)header[2]) || !isdigit((unsigned char)header[3]) || 
        header[4] != '|') {
        return 10;
    }
    
    if (header[0] != '0') {
        return 10;
    }
    
    int stated_len = (header[2] - '0') * 10 + (header[3] - '0');
    if (stated_len < 1 || stated_len > MAX_MSG_BYTES) {
        return 10;
    }

    char *payload = malloc((size_t)stated_len + 1);
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
        out->fields = (char **)calloc((size_t)out->field_count, sizeof(char*));
        if (!out->fields) {
            free(save);
            return -1;
        }
        for (int i = 0; i < out->field_count; i++) {
            out->fields[i] = strdup(tokens[i + 1]);
        }
    }
    else {
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

int is_name_in_use(const char *name) {
    pthread_mutex_lock(&global_mutex);
    for (int i = 0; i < client_count; i++) {
        if (clients[i]->is_playing && strcmp(clients[i]->name, name) == 0) {
            pthread_mutex_unlock(&global_mutex);
            return 1;
        }
    }
    pthread_mutex_unlock(&global_mutex);
    return 0;
}

void add_client(client_t *c) {
    pthread_mutex_lock(&global_mutex);
    
    if (client_count == client_cap) {
        if (client_cap == 0) {
            client_cap = INITIAL_CAP;
        } else {
            client_cap = client_cap * 2;
        }
        client_t **temp = (client_t **)realloc(clients, sizeof(client_t*) * client_cap);
        if (temp == NULL) {
            pthread_mutex_unlock(&global_mutex);
            return;
        }
        clients = temp;
    }
    
    clients[client_count++] = c;
    pthread_mutex_unlock(&global_mutex);
}

void remove_client(client_t *c) {
    pthread_mutex_lock(&global_mutex);
    for (int i = 0; i < client_count; i++) {
        if (clients[i] == c) {
            clients[i] = clients[client_count - 1];
            client_count--;
            break;
        }
    }
    pthread_mutex_unlock(&global_mutex);
}

void add_game(game_t *g) {
    pthread_mutex_lock(&global_mutex);
    
    if (game_count == game_cap) {
        if (game_cap == 0) {
            game_cap = INITIAL_CAP;
        } else {
            game_cap = game_cap * 2;
        }
        game_t **temp = (game_t **)realloc(games, sizeof(game_t*) * game_cap);
        if (temp == NULL) {
            pthread_mutex_unlock(&global_mutex);
            return;
        }
        games = temp;
    }
    
    games[game_count++] = g;
    pthread_mutex_unlock(&global_mutex);
}

void remove_game(int game_id) {
    pthread_mutex_lock(&global_mutex);
    for (int i = 0; i < game_count; i++) {
        if (games[i]->id == game_id) {
            games[i] = games[game_count - 1];
            game_count--;
            break;
        }
    }
    pthread_mutex_unlock(&global_mutex);
}

int is_board_empty(int board[5]) {
    for (int i = 0; i < 5; i++) {
        if (board[i] > 0) return 0;
    }
    return 1;
}

int validate_move(int board[5], int pile, int qty) {
    if (pile < 0 || pile >= 5) return 32;
    if (qty < 1) return 33;
    if (qty > board[pile]) return 33;
    return 0;
}

void *game_thread(void *arg) {
    game_t *game = (game_t *)arg;
    client_t *p1 = game->player1;
    client_t *p2 = game->player2;
    
    logf("Game %d started: %s vs %s", game->id, p1->name, p2->name);
    
    send_name(p1->sock, 1, p2->name);
    send_name(p2->sock, 2, p1->name);
    
    game->current_turn = 1;
    
    while (active && !game->game_over) {
        /* read current turn under lock to avoid races */
        int cur_turn;
        pthread_mutex_lock(&game->lock);
        cur_turn = game->current_turn;
        pthread_mutex_unlock(&game->lock);

        client_t *current_player = (cur_turn == 1) ? p1 : p2;
        client_t *other_player = (cur_turn == 1) ? p2 : p1;

        send_play(current_player->sock, cur_turn, game->board);
        send_play(other_player->sock, cur_turn, game->board);

        /* wait for input on either socket so we can detect impatient MOVE from other player */
        fd_set rfds;
        FD_ZERO(&rfds);
        int maxfd = -1;
        if (current_player->sock >= 0) { FD_SET(current_player->sock, &rfds); if (current_player->sock > maxfd) maxfd = current_player->sock; }
        if (other_player->sock >= 0) { FD_SET(other_player->sock, &rfds); if (other_player->sock > maxfd) maxfd = other_player->sock; }

        if (maxfd < 0) {
            /* both sockets closed? end game */
            pthread_mutex_lock(&game->lock);
            game->game_over = 1;
            game->winner = 0;
            game->forfeited = 0;
            pthread_mutex_unlock(&game->lock);
            break;
        }

        int sel = select(maxfd + 1, &rfds, NULL, NULL, NULL);
        if (sel < 0) {
            if (errno == EINTR) continue;
            break;
        }

        /* if other player sent something out-of-turn, handle it */
        if (other_player->sock >= 0 && FD_ISSET(other_player->sock, &rfds)) {
            message_t imsg;
            int r = read_ngp_message(other_player->sock, &imsg);
            if (r < 0) {
                /* other disconnected -> current player wins by forfeit */
                if (other_player->sock >= 0) { close(other_player->sock); other_player->sock = -1; }
                pthread_mutex_lock(&game->lock);
                game->game_over = 1;
                game->winner = cur_turn;
                game->forfeited = 1;
                pthread_mutex_unlock(&game->lock);
                break;
            }
            if (r > 0) {
                send_fail(other_player->sock, r, "Invalid");
                if (other_player->sock >= 0) { close(other_player->sock); other_player->sock = -1; }
                pthread_mutex_lock(&game->lock);
                game->game_over = 1;
                game->winner = cur_turn;
                game->forfeited = 0;
                pthread_mutex_unlock(&game->lock);
                break;
            }

            if (imsg.type == MSG_MOVE) {
                /* impatient move: immediate FAIL 31, keep connection open */
                send_fail(other_player->sock, 31, "Impatient");
                free_message(&imsg);
                continue; /* continue waiting for current player's move */
            } else {
                /* other message types are protocol errors during game */
                send_fail(other_player->sock, 10, "Invalid");
                if (other_player->sock >= 0) { close(other_player->sock); other_player->sock = -1; }
                pthread_mutex_lock(&game->lock);
                game->game_over = 1;
                game->winner = cur_turn;
                game->forfeited = 0;
                pthread_mutex_unlock(&game->lock);
                free_message(&imsg);
                break;
            }
        }

        /* if current player's socket is readable, handle the move */
        if (current_player->sock >= 0 && FD_ISSET(current_player->sock, &rfds)) {
            message_t msg;
            int read_result = read_ngp_message(current_player->sock, &msg);

            if (read_result < 0) {
                logf("Game %d: Player %d disconnected", game->id, cur_turn);
                if (current_player->sock >= 0) { close(current_player->sock); current_player->sock = -1; }
                pthread_mutex_lock(&game->lock);
                game->game_over = 1;
                game->winner = (cur_turn == 1) ? 2 : 1;
                game->forfeited = 1;
                pthread_mutex_unlock(&game->lock);
                break;
            }
            if (read_result > 0) {
                send_fail(current_player->sock, read_result, "Invalid");
                if (current_player->sock >= 0) { close(current_player->sock); current_player->sock = -1; }
                pthread_mutex_lock(&game->lock);
                game->game_over = 1;
                game->winner = (cur_turn == 1) ? 2 : 1;
                game->forfeited = 0;
                pthread_mutex_unlock(&game->lock);
                break;
            }

            if (msg.type != MSG_MOVE) {
                send_fail(current_player->sock, 10, "Invalid");
                if (current_player->sock >= 0) { close(current_player->sock); current_player->sock = -1; }
                pthread_mutex_lock(&game->lock);
                game->game_over = 1;
                game->winner = (cur_turn == 1) ? 2 : 1;
                game->forfeited = 0;
                pthread_mutex_unlock(&game->lock);
                free_message(&msg);
                break;
            }

            /* handle MOVE as before */
            if (msg.field_count != 2) {
                send_fail(current_player->sock, 10, "Invalid");
                if (current_player->sock >= 0) { close(current_player->sock); current_player->sock = -1; }
                pthread_mutex_lock(&game->lock);
                game->game_over = 1;
                game->winner = (cur_turn == 1) ? 2 : 1;
                game->forfeited = 0;
                pthread_mutex_unlock(&game->lock);
                free_message(&msg);
                break;
            }

            char *end;
            long pile_val = strtol(msg.fields[0], &end, 10);
            if (*end != '\0' || pile_val < 1 || pile_val > 5) {
                send_fail(current_player->sock, 32, "Pile Index");
                free_message(&msg);
                continue;
            }

            long qty_val = strtol(msg.fields[1], &end, 10);
            if (*end != '\0' || qty_val < 1 || qty_val > INT_MAX) {
                send_fail(current_player->sock, 33, "Quantity");
                free_message(&msg);
                continue;
            }

            int pile = (int)(pile_val - 1);
            int qty = (int)qty_val;

            pthread_mutex_lock(&game->lock);
            int valid = validate_move(game->board, pile, qty);
            if (valid != 0) {
                pthread_mutex_unlock(&game->lock);
                int code = (valid == 32) ? 32 : 33;
                const char *err = (valid == 32) ? "Pile Index" : "Quantity";
                send_fail(current_player->sock, code, err);
                free_message(&msg);
                continue;
            }

            game->board[pile] -= qty;
            logf("Game %d: Player %d removed %d from pile %d", game->id, cur_turn, qty, pile);

            if (is_board_empty(game->board)) {
                game->game_over = 1;
                game->winner = cur_turn;
                game->forfeited = 0;
                logf("Game %d: Player %d won", game->id, game->winner);
            } else {
                game->current_turn = (cur_turn == 1) ? 2 : 1;
            }
            pthread_mutex_unlock(&game->lock);

            free_message(&msg);
        }
    }
    
    if (game->game_over) {
        if (p1->sock >= 0) send_over(p1->sock, game->winner, game->board, game->forfeited);
        if (p2->sock >= 0) send_over(p2->sock, game->winner, game->board, game->forfeited);
    }
    
    if (p1->sock >= 0) { close(p1->sock); p1->sock = -1; }
    if (p2->sock >= 0) { close(p2->sock); p2->sock = -1; }
    
    remove_client(p1);
    remove_client(p2);
    remove_game(game->id);
    
    free(p1->name);
    free(p2->name);
    free(p1);
    free(p2);
    free(game);
    
    return NULL;
}

void add_wait(client_t *c) {
    /* add client to global list */
    add_client(c);
    pthread_mutex_lock(&global_mutex);

    // Check if we can match this player with a waiting opponent
    client_t *opponent = NULL;
    for (int i = 0; i < client_count; i++) {
        if (clients[i] != c && clients[i]->is_waiting && !clients[i]->is_playing) {
            opponent = clients[i];
            clients[i]->is_waiting = 0;
            break;
        }
    }

    if (opponent) {
        // Found opponent, start a game
        c->is_waiting = 0;
        c->is_playing = 1;
        opponent->is_playing = 1;
        
        game_t *game = (game_t *)malloc(sizeof(game_t));
        if (!game) {
            fprintf(stderr, "malloc for game_t failed. Disconnecting clients.\n");
            close(c->sock);
            close(opponent->sock);
            free(c->name);
            free(opponent->name);
            free(c);
            free(opponent);
            remove_client(c);
            remove_client(opponent);
            pthread_mutex_unlock(&global_mutex);
            return;
        }

        game->id = next_game_id++;
        game->player1 = c;
        game->player2 = opponent;
        game->board[0] = 1;
        game->board[1] = 3;
        game->board[2] = 5;
        game->board[3] = 7;
        game->board[4] = 9;
        game->current_turn = 1;
        game->game_over = 0;
        game->winner = 0;
        game->forfeited = 0;
        pthread_mutex_init(&game->lock, NULL);
        
        c->game_id = game->id;
        opponent->game_id = game->id;
        
        add_game(game);
        
        pthread_t tid;
        if (pthread_create(&tid, NULL, game_thread, game) != 0) {
            perror("pthread_create failed");
            free(game);
            pthread_mutex_unlock(&global_mutex);
            return;
        }
        pthread_detach(tid);
    } else {
        // No opponent found, mark as waiting
        c->is_waiting = 1;
        send_wait(c->sock);
    }
    pthread_mutex_unlock(&global_mutex);
}

void handle_client(int sock) {
    message_t msg;
    int msg_result;
    
    msg_result = read_ngp_message(sock, &msg);
    
    if (msg_result < 0) {
        close(sock);
        return;
    }
    
    if (msg_result > 0 || msg.type != MSG_OPEN) {
        send_fail(sock, 10, "Invalid");
        close(sock);
        return;
    }
    
    if (msg.field_count != 1) {
        send_fail(sock, 10, "Invalid");
        close(sock);
        free_message(&msg);
        return;
    }
    
    const char *name = msg.fields[0];
    int name_valid = validate_name_field(name);
    
    if (name_valid != 0) {
        if (name_valid == 21) {
            send_fail(sock, 21, "Long Name");
        } else {
            send_fail(sock, 10, "Invalid");
        }
        close(sock);
        free_message(&msg);
        return;
    }
    
    if (is_name_in_use(name)) {
        send_fail(sock, 22, "Already Playing");
        close(sock);
        free_message(&msg);
        return;
    }
    
    client_t *c = (client_t *)malloc(sizeof(client_t));
    if (!c) {
        send_fail(sock, 10, "Invalid");
        close(sock);
        free_message(&msg);
        return;
    }
    
    c->sock = sock;
    c->name = strdup(name);
    c->game_id = -1;
    c->player_num = 0;
    c->is_waiting = 0;
    c->is_playing = 0; /* not yet in a game */
    
    free_message(&msg);
    
    logf("Player %s connected", c->name);
    add_wait(c);
}

/* wrapper used by pthread_create */
void *client_thread(void *arg) {
    int sock = (intptr_t)arg;
    handle_client(sock);
    return NULL;
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

    logf("Nim server listening on port %s", service);

    while (active) {
        struct sockaddr_storage remote_host;
        socklen_t remote_host_len = sizeof(remote_host);

        int sock = accept(listener, (struct sockaddr *)&remote_host, &remote_host_len);
        if (sock < 0) {
            if (active) perror("accept failed");
            continue;
        }
        
        pthread_t tid;
        if (pthread_create(&tid, NULL, client_thread, (void *)(intptr_t)sock) != 0) {
            perror("pthread_create failed");
            close(sock);
            continue;
        }
        pthread_detach(tid);
    }

    close(listener);
    
    pthread_mutex_lock(&global_mutex);
    for (int i = 0; i < client_count; i++) {
        close(clients[i]->sock);
        free(clients[i]->name);
        free(clients[i]);
    }
    free(clients);
    
    for (int i = 0; i < game_count; i++) {
        pthread_mutex_destroy(&games[i]->lock);
        free(games[i]);
    }
    free(games);
    pthread_mutex_unlock(&global_mutex);
    
    return EXIT_SUCCESS;
}
