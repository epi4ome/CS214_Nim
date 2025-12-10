#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <ctype.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/select.h> 
#include <netdb.h>
#include <signal.h>
#include <time.h> 

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
int next_gid = 1;

pthread_mutex_t global_mutex = PTHREAD_MUTEX_INITIALIZER;

int act_client_thrds = 0;
int act_game_thrds = 0;
pthread_mutex_t active_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t active_cond = PTHREAD_COND_INITIALIZER;

static void print_message(const char *direction, const char *raw_msg) {
    printf("%s: %s\n", direction, raw_msg);
    fflush(stdout);
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
    signal(SIGPIPE, SIG_IGN);
}

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
    buf[total] = '\0';

    printf("Checkpoint: check buffer: %s\n", buf);
    print_message("SENT", buf);

    ssize_t sent = 0;
    while ((size_t)sent < (size_t)total) {
        printf("Checkpoint: sent so far: %zd bytes\ntotal: %d bytes\n", sent, total);
        ssize_t w = send(fd, buf + sent, (size_t)total - (size_t)sent, 0);
        printf("Checkpoint: send returned: %zd\n", w);
        if (w < 0) {
            if (errno == EINTR) continue;
            free(buf);
            return -1;
        }
        sent += w;
        printf("Checkpoint: sent so far: %zd bytes\ntotal: %d bytes\n", sent, total);
    }
    free(buf);
    printf("Checkpoint: sent %zd bytes\n", sent);
    return sent;
}

static void send_fail(int fd, int code, const char *msg) {
    char payload[256];
    int n = snprintf(payload, sizeof(payload), "FAIL|%d %s|", code, msg);
    if (n < 0) return;
    if (n > MAX_MSG_BYTES) return; 
    printf("Checkpoint: payload: %s\n", payload);
    send_ngp(fd, payload);
}

static void send_wait(int fd) {
    send_ngp(fd, "WAIT|");
}

static void send_name(client_t *c, int role, const char *opp_name) {
    char payload[128];
    snprintf(payload, sizeof(payload), "NAME|%d|%s|", role, opp_name);
    send_ngp(c->sock, payload);
}

static void send_play(client_t *c, int next_role, int board[5]) {
    char b[64];
    snprintf(b, sizeof(b), "%d %d %d %d %d", board[0], board[1], board[2], board[3], board[4]);
    char payload[256];
    snprintf(payload, sizeof(payload), "PLAY|%d|%s|", next_role, b);
    send_ngp(c->sock, payload);
}

static void send_over(client_t *c, int winner, int board[5], int forfeited) {
    char b[64];
    snprintf(b, sizeof(b), "%d %d %d %d %d", board[0], board[1], board[2], board[3], board[4]);
    char payload[256];
    if (forfeited) {
        snprintf(payload, sizeof(payload), "OVER|%d|%s|Forfeit|", winner, b);
    } 
    else {
        snprintf(payload, sizeof(payload), "OVER|%d|%s||", winner, b);
    }
    send_ngp(c->sock, payload);
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

static int read_message(int fd, message_t *out) {
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

    if (header[1] != '|' || header[4] != '|' || 
        header[0] != '0' ||
        !isdigit((unsigned char)header[2]) || 
        !isdigit((unsigned char)header[3])) {
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
    payload[stated_len] = '\0'; 

    char full_msg[5 + MAX_MSG_BYTES + 1]; 
    if (stated_len <= MAX_MSG_BYTES) {
        memcpy(full_msg, header, 5);
        memcpy(full_msg + 5, payload, stated_len + 1);
        print_message("RECV", full_msg);
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
    
    free(save);
    return 0;
}

static int name_valid(const char *name) {
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

int name_check_status(const char *name) {
    pthread_mutex_lock(&global_mutex);

    for (int i = 0; i < client_count; i++) {
        if (clients[i]->name != NULL && strcmp(clients[i]->name, name) == 0) {
            pthread_mutex_unlock(&global_mutex);
            return 23;
        }
    }
    
    for (int i = 0; i < game_count; i++) {
        game_t *g = games[i];
        pthread_mutex_lock(&g->lock); 
        
        if (g->player1 != NULL && g->player1->name != NULL && strcmp(g->player1->name, name) == 0) {
            pthread_mutex_unlock(&g->lock);
            pthread_mutex_unlock(&global_mutex);
            return 22;
        }
        if (g->player2 != NULL && g->player2->name != NULL && strcmp(g->player2->name, name) == 0) {
            pthread_mutex_unlock(&g->lock);
            pthread_mutex_unlock(&global_mutex);
            return 22;
        }
        
        pthread_mutex_unlock(&g->lock);
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
    for (int i = 0; i < client_count; i++) {
        if (clients[i] == c) {
            clients[i] = clients[client_count - 1];
            client_count--;
            break;
        }
    }
}

void add_game(game_t *g) {
    if (game_count == game_cap) {
        if (game_cap == 0) {
            game_cap = INITIAL_CAP;
        } else {
            game_cap = game_cap * 2;
        }
        game_t **temp = (game_t **)realloc(games, sizeof(game_t*) * game_cap);
        if (temp == NULL) {
            return;
        }
        games = temp;
    }
    
    games[game_count++] = g;
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

    send_name(p1, 1, p2->name);
    send_name(p2, 2, p1->name);
    
    game->current_turn = 1;

    while (active && !game->game_over) {
        int cur_turn;
        pthread_mutex_lock(&game->lock);
        cur_turn = game->current_turn;
        pthread_mutex_unlock(&game->lock);

        client_t *current_player;
        client_t *other_player;

        if (cur_turn == 1) {
            current_player = p1;
            other_player = p2;
        } else {
            current_player = p2;
            other_player = p1;
        }

        if (current_player->sock >= 0) send_play(current_player, cur_turn, game->board);
        if (other_player->sock >= 0) send_play(other_player, cur_turn, game->board);

        fd_set rfds;
        FD_ZERO(&rfds);
        int maxfd = -1;
        
        if (current_player->sock >= 0) {
            FD_SET(current_player->sock, &rfds);
            if (current_player->sock > maxfd) {
                maxfd = current_player->sock;
            }
        }
        if (other_player->sock >= 0) { 
            FD_SET(other_player->sock, &rfds);
            if (other_player->sock > maxfd) {
                maxfd = other_player->sock;
            }
        }

        if (maxfd < 0) {
            pthread_mutex_lock(&game->lock);
            game->game_over = 1;
            game->winner = 0;
            game->forfeited = 0;
            pthread_mutex_unlock(&game->lock);
            break;
        }

        int sel = select(maxfd + 1, &rfds, NULL, NULL, NULL); 
        
        if (sel < 0) {
            if (errno == EINTR) {
                if (!active) break;
                continue;
            }
            break;
        }
       
        if (other_player->sock >= 0 && FD_ISSET(other_player->sock, &rfds)) {
            message_t imsg;
            int r = read_message(other_player->sock, &imsg);
            
            if (r == 0 && imsg.type == MSG_MOVE) {
                send_fail(other_player->sock, 31, "Impatient");
                free_message(&imsg);
                continue; 
            }
            
            if (other_player->sock >= 0) { close(other_player->sock); other_player->sock = -1; }
        
            pthread_mutex_lock(&game->lock);
            game->game_over = 1;
            game->winner = cur_turn;
            game->forfeited = 1; 
            pthread_mutex_unlock(&game->lock);
            if (r > 0) free_message(&imsg);
            break;
        }

        if (current_player->sock >= 0 && FD_ISSET(current_player->sock, &rfds)) {
            message_t msg;
            int read_result = read_message(current_player->sock, &msg);

            if (read_result < 0) {
                if (current_player->sock >= 0) {
                    close(current_player->sock);
                    current_player->sock = -1;
                }
                pthread_mutex_lock(&game->lock);
                game->game_over = 1;
                if (cur_turn == 1) game->winner = 2;
                else game->winner = 1;
                game->forfeited = 1;
                pthread_mutex_unlock(&game->lock);
                break;
            } else if (read_result > 0) {
                send_fail(current_player->sock, read_result, "Invalid");
                continue;
            }

            if (msg.type != MSG_MOVE) {
                send_fail(current_player->sock, 10, "Invalid");
                free_message(&msg);
                continue;
            }

            if (msg.field_count != 2) {
                send_fail(current_player->sock, 10, "Invalid");
                free_message(&msg);
                continue;
            }

            char *end;
            long pile_val = strtol(msg.fields[0], &end, 10);
            
            if (*end != '\0' || pile_val < 0 || pile_val > 4) {
                send_fail(current_player->sock, 32, "Pile Index");
                free_message(&msg);
                continue;
            }
            int pile = (int)pile_val; 

            long qty_val = strtol(msg.fields[1], &end, 10);
            if (*end != '\0' || qty_val < 1 || qty_val > INT_MAX) {
                send_fail(current_player->sock, 33, "Quantity");
                free_message(&msg);
                continue;
            }
            int qty = (int)qty_val;

            pthread_mutex_lock(&game->lock);
            int valid = validate_move(game->board, pile, qty);
            
            if (valid != 0) {
                pthread_mutex_unlock(&game->lock);
                if (valid == 32) send_fail(current_player->sock, 32, "Pile Index");
                else send_fail(current_player->sock, 33, "Quantity");
                free_message(&msg);
                continue;
            }

            game->board[pile] -= qty;

            if (is_board_empty(game->board)) {
                game->game_over = 1;
                game->winner = cur_turn;
                game->forfeited = 0;
            } else {
                game->current_turn = (cur_turn == 1) ? 2 : 1;
            }
            pthread_mutex_unlock(&game->lock);

            free_message(&msg);
            
            if (game->game_over && game->forfeited == 0) {
                break;
            }
        }
    }

    if (game->game_over) {
        if (p1->sock >= 0) send_over(p1, game->winner, game->board, game->forfeited);
        if (p2->sock >= 0) send_over(p2, game->winner, game->board, game->forfeited);
    }

    if (p1->sock >= 0) { close(p1->sock); p1->sock = -1; }
    if (p2->sock >= 0) { close(p2->sock); p2->sock = -1; }

    free(p1->name);
    free(p2->name);
    free(p1);
    free(p2);
    remove_game(game->id);
    free(game);

    pthread_mutex_lock(&active_mutex);
    act_game_thrds--;
    pthread_cond_signal(&active_cond);
    pthread_mutex_unlock(&active_mutex);

    return NULL;
}

void add_wait(client_t *c) {
    add_client(c);
    pthread_mutex_lock(&global_mutex);

    client_t *opponent = NULL;
    for (int i = 0; i < client_count; i++) {
        if (clients[i] != c && clients[i]->is_waiting && !clients[i]->is_playing) {
            opponent = clients[i];
            break;
        }
    }

    if (opponent) {
        send_wait(c->sock); 
        remove_client(c);
        remove_client(opponent); 

        c->is_waiting = 0;
        c->is_playing = 1;
        opponent->is_waiting = 0;
        opponent->is_playing = 1;

        game_t *game = (game_t *)malloc(sizeof(game_t));
        if (!game) {
            close(c->sock);
            close(opponent->sock);
            free(c->name);
            free(opponent->name);
            free(c);
            free(opponent);
            pthread_mutex_unlock(&global_mutex);
            return;
        }

        game->id = next_gid++;
        game->player1 = opponent;
        game->player2 = c; 
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

        pthread_mutex_lock(&active_mutex);
        act_game_thrds++;
        pthread_mutex_unlock(&active_mutex);
        pthread_t tid;
        int err = pthread_create(&tid, NULL, game_thread, game);
        if (err != 0) {
            close(c->sock);
            close(opponent->sock);
            free(c->name);
            free(opponent->name);
            free(c);
            free(opponent);
            remove_game(game->id);
            free(game);
            pthread_mutex_lock(&active_mutex);
            act_game_thrds--;
            pthread_mutex_unlock(&active_mutex);
            pthread_mutex_unlock(&global_mutex);
            return;
        }
        pthread_detach(tid);

    } else {
        c->is_waiting = 1;
        send_wait(c->sock);
    }

    pthread_mutex_unlock(&global_mutex);
}

void handle_error_23_name(const char *name) {
    pthread_mutex_lock(&global_mutex);
    for (int i = 0; i < client_count; i++) {
        client_t *c = clients[i];
        if (!c) continue;
        if (c->name && strcmp(c->name, name) == 0 && c->is_waiting && !c->is_playing) {
            if (c->sock >= 0) {
                /* shutdown wakes the other thread's select/recv but leaves FD details
                   for that thread to close and free safely. Don't set c->sock = -1 here. */
                shutdown(c->sock, SHUT_RDWR);
            }
            break;
        }
    }
    pthread_mutex_unlock(&global_mutex);
}

client_t *handle_client(int sock) {
    message_t msg;
    int msg_result;
    
    msg_result = read_message(sock, &msg);
    
    if (msg_result < 0) {
        close(sock);
        return NULL;
    }

    if (msg.type == MSG_MOVE) {
        send_fail(sock, 24, "Not Playing");
        close(sock);
        free_message(&msg);
        return NULL;
    }

    if (msg_result > 0 || msg.type != MSG_OPEN) {
        send_fail(sock, 10, "Invalid");
        close(sock);
        free_message(&msg);
        return NULL;
    }
    
    if (msg.field_count != 1) {
        send_fail(sock, 10, "Invalid");
        close(sock);
        free_message(&msg);
        return NULL;
    }
    
    const char *name = msg.fields[0];
    int name_check = name_valid(name);
    
    if (name_check != 0) {
        if (name_check == 21) {
            send_fail(sock, 21, "Long Name");
        } else {
            send_fail(sock, 10, "Invalid");
        }
        close(sock);
        free_message(&msg);
        return NULL;
    }
    
    int status_check = name_check_status(name);
    if (status_check != 0) {
        if (status_check == 22) {
            send_fail(sock, 22, "Already Playing");
            sleep(1);

            /* Already Playing: just close the new client's socket */
            free_message(&msg);
            close(sock);
            printf("Checkpoint: fail 22\n");
            return NULL;
        } else { /* 23 */
            /* Send FAIL to the new connection first (blocking send_ngp) */
            send_fail(sock, 23, "Already Open");
            /* Wake the waiting client so its thread will cleanup (and remove/free) */
            handle_error_23_name(name);

            /* Now close the new client's socket and return */
            free_message(&msg);
            close(sock);
            printf("Checkpoint: fail 23\n");
            return NULL;
        }
    }

    
    client_t *c = (client_t *)malloc(sizeof(client_t));
    if (!c) {
        send_fail(sock, 10, "Invalid");
        close(sock);
        free_message(&msg);
        return NULL;
    }
    
    c->sock = sock;
    c->name = strdup(name);
    c->game_id = -1;
    c->player_num = 0;
    c->is_waiting = 0;
    c->is_playing = 0; 
    
    free_message(&msg);
    add_wait(c);

    if (c->is_playing) { 
        return NULL; 
    }
    return c;
}


void *client_thread(void *arg) {
    int sock = (intptr_t)arg;
    client_t *c = handle_client(sock);

    if (c != NULL) {
        fd_set rfds;
        struct timeval tv;
        int break_loop = 0;
        while (active && c->is_waiting && !break_loop) {
            tv.tv_sec = 0;
            tv.tv_usec = 100000; 
            FD_ZERO(&rfds);
            FD_SET(c->sock, &rfds);

            int sel = select(c->sock + 1, &rfds, NULL, NULL, &tv);

            if (sel < 0) {
                if (errno != EINTR) break;
            } else if (sel > 0 && FD_ISSET(c->sock, &rfds)) {
                message_t msg;
                int read_result = read_message(c->sock, &msg);

                if (read_result < 0) {
                    break;
                }
                
                int fail_code;
                const char *fail_msg;

                if (msg.type == MSG_OPEN) {
                    fail_code = 23;
                    fail_msg = "Already Open";
                } else if (msg.type == MSG_MOVE) {
                    fail_code = 24;
                    fail_msg = "Not Playing";
                } else {
                    fail_code = 10;
                    fail_msg = "Invalid Message";
                }

                send_fail(c->sock, fail_code, fail_msg);
                
                free_message(&msg);
                break_loop = 1;
            }
        }

        pthread_mutex_lock(&global_mutex);
        if (c->is_waiting) { 
            close(c->sock);
            remove_client(c);
            free(c->name);
            free(c);
        }
        pthread_mutex_unlock(&global_mutex);
    }
    
    pthread_mutex_lock(&active_mutex);
    act_client_thrds--;
    pthread_cond_signal(&active_cond);  
    pthread_mutex_unlock(&active_mutex);
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

    printf("Nim server listening on port %s\n", service); 

    while (active) {
        struct sockaddr_storage remote_host;
        socklen_t remote_host_len = sizeof(remote_host);

        int sock = accept(listener, (struct sockaddr *)&remote_host, &remote_host_len);
        if (sock < 0) {
            if (errno == EINTR && !active) {
                break; 
            }
            if (active) perror("accept failed");
            continue;
        }

        pthread_mutex_lock(&active_mutex);
        act_client_thrds++;  
        pthread_mutex_unlock(&active_mutex);

        pthread_t tid;
        if (pthread_create(&tid, NULL, client_thread, (void *)(intptr_t)sock) != 0) {
            perror("pthread_create failed");
            close(sock);
            pthread_mutex_lock(&active_mutex);
            act_client_thrds--; 
            pthread_mutex_unlock(&active_mutex);
            continue;
        }
        pthread_detach(tid);
    }

    close(listener);
    
    pthread_mutex_lock(&global_mutex);
    pthread_mutex_lock(&active_mutex);
    while (act_client_thrds > 0 || act_game_thrds > 0) {
        pthread_cond_wait(&active_cond, &active_mutex);
    }
    pthread_mutex_unlock(&active_mutex);

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