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

/* Constants from the spec */
#define QUEUE 10
#define MAX_PAYLOAD 99   /* payload (stated_len) max */
#define MAX_TOTAL   104  /* header (5) + payload <= 104 */
#define MAX_NAME_LEN 72
#define INITIAL_CAP 10

volatile int active = 1;

/* Message types */
typedef enum {
    MSG_OPEN, MSG_WAIT, MSG_NAME, MSG_PLAY, MSG_MOVE, MSG_OVER, MSG_FAIL, MSG_INVALID
} msg_type_t;

/* Parsed message structure */
typedef struct {
    msg_type_t type;
    int field_count;
    char **fields;
} message_t;

/* Client and Game structures */
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

/* Global lists protected by global_mutex */
client_t **clients = NULL;
int client_count = 0;
int client_cap = 0;

game_t **games = NULL;
int game_count = 0;
int game_cap = 0;
int next_game_id = 1;

pthread_mutex_t global_mutex = PTHREAD_MUTEX_INITIALIZER;

/* -------------------------------------------------------------------------- */
/* Logging helper */
static void nim_logf(const char *fmt, ...) {
    if (!fmt) return;
    va_list ap;
    va_start(ap, fmt);
    vprintf(fmt, ap);
    printf("\n");
    fflush(stdout);
    va_end(ap);
}

/* Signal handler to stop server */
void handler(int signum) {
    (void)signum;
    active = 0;
}

/* -------------------------------------------------------------------------- */
/* Networking helpers */

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
        if (listener_sock < 0) {
            continue;
        }
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

/* -------------------------------------------------------------------------- */
/* Protocol encoding/decoding */

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

/* send_ngp: build "0|LL|<payload>" and send, returns bytes sent or -1 */
static ssize_t send_ngp(int fd, const char *payload) {
    if (!payload) return -1;
    int len = (int)strlen(payload);
    if (len > MAX_PAYLOAD) {
        return -1;
    }
    char header[8];
    int hlen = snprintf(header, sizeof(header), "0|%02d|", len);
    if (hlen < 0) {
        return -1;
    }
    int total = hlen + len;
    if (total > MAX_TOTAL) {
        return -1;
    }
    char *buf = malloc((size_t)total + 1);
    if (!buf) return -1;
    memcpy(buf, header, (size_t)hlen);
    memcpy(buf + hlen, payload, (size_t)len);
    ssize_t sent = 0;
    while (sent < total) {
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

/* Safe wrappers that check length and log errors */
static void send_fail(int fd, int code, const char *msg) {
    if (fd < 0) return;
    char payload[256];
    int n = snprintf(payload, sizeof(payload), "FAIL|%d %s|", code, msg);
    if (n < 0) return;
    if (n > MAX_PAYLOAD) {
        /* fallback to minimal code-only message */
        snprintf(payload, sizeof(payload), "FAIL|%d|", code);
    }
    if (send_ngp(fd, payload) < 0) {
        nim_logf("send_fail: send_ngp failed (fd=%d)", fd);
    }
}

static void send_wait(int fd) {
    if (fd < 0) return;
    if (send_ngp(fd, "WAIT|") < 0) nim_logf("send_wait: send failed (fd=%d)", fd);
}

static void send_name(int fd, int role, const char *name) {
    if (fd < 0) return;
    char payload[256];
    int n = snprintf(payload, sizeof(payload), "NAME|%d|%s|", role, name);
    if (n < 0 || n > MAX_PAYLOAD) {
        nim_logf("send_name: payload too long (role=%d)", role);
        return;
    }
    if (send_ngp(fd, payload) < 0) nim_logf("send_name: send failed (fd=%d)", fd);
}

static void send_play(int fd, int next_role, int board[5]) {
    if (fd < 0) return;
    char b[64];
    snprintf(b, sizeof(b), "%d %d %d %d %d", board[0], board[1], board[2], board[3], board[4]);
    char payload[256];
    int n = snprintf(payload, sizeof(payload), "PLAY|%d|%s|", next_role, b);
    if (n < 0 || n > MAX_PAYLOAD) {
        nim_logf("send_play: payload too long");
        return;
    }
    if (send_ngp(fd, payload) < 0) nim_logf("send_play: send failed (fd=%d)", fd);
}

static void send_over(int fd, int winner, int board[5], int forfeited) {
    if (fd < 0) return;
    char b[64];
    snprintf(b, sizeof(b), "%d %d %d %d %d", board[0], board[1], board[2], board[3], board[4]);
    char payload[256];
    if (forfeited) {
        snprintf(payload, sizeof(payload), "OVER|%d|%s|Forfeit|", winner, b);
    } else {
        snprintf(payload, sizeof(payload), "OVER|%d|%s||", winner, b);
    }
    if ((int)strlen(payload) > MAX_PAYLOAD) {
        nim_logf("send_over: payload too long");
        return;
    }
    if (send_ngp(fd, payload) < 0) nim_logf("send_over: send failed (fd=%d)", fd);
}

/* -------------------------------------------------------------------------- */
/* Message parsing */

/* free parsed message */
static void free_message(message_t *msg) {
    if (!msg) return;
    if (msg->fields) {
        for (int i = 0; i < msg->field_count; ++i) free(msg->fields[i]);
        free(msg->fields);
    }
    msg->fields = NULL;
    msg->field_count = 0;
    msg->type = MSG_INVALID;
}

/*
 Read one NGP message from fd into out.
 Returns:
  0  => success (out filled)
 -1  => IO error / disconnect
 >0  => protocol/presentation error code (e.g., 10)
*/
static int read_ngp_message(int fd, message_t *out) {
    if (!out) return -1;
    out->field_count = 0;
    out->fields = NULL;
    out->type = MSG_INVALID;

    /* header buffer - make a little larger for safety */
    char header[8];
    ssize_t r;
    size_t got = 0;

    /* read exactly 5 header bytes: V|DD| */
    while (got < 5) {
        r = recv(fd, header + got, 5 - got, 0);
        if (r <= 0) return -1; /* disconnect or error */
        got += (size_t)r;
    }

    if (!isdigit((unsigned char)header[0]) || header[1] != '|' ||
        !isdigit((unsigned char)header[2]) || !isdigit((unsigned char)header[3]) ||
        header[4] != '|') {
        return 10; /* framing/presentation error */
    }

    if (header[0] != '0') { /* protocol version mismatch */
        return 10;
    }

    int stated_len = (header[2] - '0') * 10 + (header[3] - '0');

    /* Basic stated_len validation */
    if (stated_len < 5 || stated_len > MAX_PAYLOAD) {
        return 10;
    }

    /* Validate total size doesn't exceed the allowed total */
    if (5 + stated_len > MAX_TOTAL) {
        return 10;
    }

    char *payload = malloc((size_t)stated_len + 1);
    if (!payload) return -1;

    size_t off = 0;
    while (off < (size_t)stated_len) {
        r = recv(fd, payload + off, (size_t)stated_len - off, 0);
        if (r <= 0) { free(payload); return -1; }
        off += (size_t)r;
    }
    payload[stated_len] = '\0';

    /* payload must end with '|' */
    if (payload[stated_len - 1] != '|') {
        free(payload);
        return 10;
    }

    /* Tokenize on '|' keeping empty fields */
    char *tokens[64];
    int count = 0;
    char *p = payload;
    char *start = p;
    for (size_t i = 0; i < (size_t)stated_len; ++i) {
        if (p[i] == '|') {
            size_t len = (p + i) - start;
            char *tok = malloc(len + 1);
            if (!tok) {
                for (int j = 0; j < count; ++j) free(tokens[j]);
                free(payload);
                return -1;
            }
            if (len > 0) memcpy(tok, start, len);
            tok[len] = '\0';
            if (count < (int)(sizeof(tokens) / sizeof(tokens[0]))) {
                tokens[count++] = tok;
            } else {
                free(tok);
                for (int j = 0; j < count; ++j) free(tokens[j]);
                free(payload);
                return 10;
            }
            start = p + i + 1;
        }
    }

    free(payload);

    if (count < 1) {
        for (int j = 0; j < count; ++j) free(tokens[j]);
        return 10;
    }

    out->type = decode_type(tokens[0]);
    if (out->type == MSG_INVALID) {
        for (int j = 0; j < count; ++j) free(tokens[j]);
        return 10;
    }

    out->field_count = count - 1;
    if (out->field_count > 0) {
        out->fields = calloc((size_t)out->field_count, sizeof(char*));
        if (!out->fields) {
            for (int j = 0; j < count; ++j) free(tokens[j]);
            return -1;
        }
        for (int i = 0; i < out->field_count; ++i) {
            out->fields[i] = tokens[i + 1]; /* transfer ownership */
        }
    } else {
        out->fields = NULL;
    }
    free(tokens[0]); /* free type token */
    return 0;
}

/* -------------------------------------------------------------------------- */
/* Validation helpers */

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

/* check if name is in use by any currently-playing client */
int is_name_in_use(const char *name) {
    pthread_mutex_lock(&global_mutex);
    for (int i = 0; i < client_count; ++i) {
        if (clients[i]->is_playing && strcmp(clients[i]->name, name) == 0) {
            pthread_mutex_unlock(&global_mutex);
            return 1;
        }
    }
    pthread_mutex_unlock(&global_mutex);
    return 0;
}

/* -------------------------------------------------------------------------- */
/* Global list helpers (caller must hold global_mutex for locked variants) */

void ensure_client_capacity_locked(void) {
    if (client_count == client_cap) {
        int newcap = (client_cap == 0) ? INITIAL_CAP : client_cap * 2;
        client_t **temp = (client_t **)realloc(clients, sizeof(client_t*) * newcap);
        if (!temp) return; /* caller handles failure */
        clients = temp;
        client_cap = newcap;
    }
}

void ensure_game_capacity_locked(void) {
    if (game_count == game_cap) {
        int newcap = (game_cap == 0) ? INITIAL_CAP : game_cap * 2;
        game_t **temp = (game_t **)realloc(games, sizeof(game_t*) * newcap);
        if (!temp) return;
        games = temp;
        game_cap = newcap;
    }
}

void add_game_locked(game_t *g) {
    if (!g) return;
    ensure_game_capacity_locked();
    if (game_count < game_cap) {
        games[game_count++] = g;
    } else {
        /* last-resort growth attempt */
        int newcap = (game_cap == 0) ? INITIAL_CAP : game_cap * 2;
        game_t **temp = (game_t **)realloc(games, sizeof(game_t*) * newcap);
        if (temp) {
            games = temp;
            game_cap = newcap;
            games[game_count++] = g;
        } else {
            nim_logf("add_game_locked: failed to resize game array");
        }
    }
}

void remove_client_locked(client_t *c) {
    for (int i = 0; i < client_count; ++i) {
        if (clients[i] == c) {
            clients[i] = clients[client_count - 1];
            client_count--;
            break;
        }
    }
}

void remove_game_locked(int game_id) {
    for (int i = 0; i < game_count; ++i) {
        if (games[i]->id == game_id) {
            games[i] = games[game_count - 1];
            game_count--;
            break;
        }
    }
}

/* -------------------------------------------------------------------------- */
/* Nim rules helpers */

int is_board_empty(int board[5]) {
    for (int i = 0; i < 5; ++i) if (board[i] > 0) return 0;
    return 1;
}

/* validate move: 0 ok, 32 pile error, 33 qty error */
int validate_move(int board[5], int pile, int qty) {
    if (pile < 0 || pile >= 5) return 32;
    if (qty <= 0) return 33;
    if (board[pile] < 0) return 33; /* defensive */
    if (qty > board[pile]) return 33;
    return 0;
}

/* -------------------------------------------------------------------------- */
/* Per-game thread (implements extra-credit behavior) */

void *game_thread(void *arg) {
    game_t *game = (game_t *)arg;
    client_t *p1 = game->player1;
    client_t *p2 = game->player2;

    nim_logf("Game %d started: %s vs %s", game->id, p1->name, p2->name);

    /* Send NAME to both players (their player number and opponent name) */
    send_name(p1->sock, 1, p2->name);
    send_name(p2->sock, 2, p1->name);

    /* initial turn */
    pthread_mutex_lock(&game->lock);
    game->current_turn = 1;
    pthread_mutex_unlock(&game->lock);

    while (active && !game->game_over) {
        int cur_turn;
        pthread_mutex_lock(&game->lock);
        cur_turn = game->current_turn;
        pthread_mutex_unlock(&game->lock);

        client_t *current_player = (cur_turn == 1) ? p1 : p2;
        client_t *other_player   = (cur_turn == 1) ? p2 : p1;

        /* Send PLAY to both clients (spec indicates PLAY is sent to both) */
        send_play(current_player->sock, cur_turn, game->board);
        send_play(other_player->sock, cur_turn, game->board);

        /* Build fd_set for select so we can detect impatient MOVE from other player */
        fd_set rfds;
        FD_ZERO(&rfds);
        int maxfd = -1;
        if (current_player->sock >= 0) { FD_SET(current_player->sock, &rfds); if (current_player->sock > maxfd) maxfd = current_player->sock; }
        if (other_player->sock >= 0)   { FD_SET(other_player->sock, &rfds);   if (other_player->sock > maxfd)   maxfd = other_player->sock; }

        if (maxfd < 0) {
            /* both sockets closed => end game (no winner) */
            pthread_mutex_lock(&game->lock);
            game->game_over = 1;
            game->winner = 0;
            game->forfeited = 0;
            pthread_mutex_unlock(&game->lock);
            break;
        }

        /* check FD_SETSIZE bounds */
        if (maxfd >= FD_SETSIZE) {
            nim_logf("Error: socket fd %d >= FD_SETSIZE (%d)", maxfd, FD_SETSIZE);
            break;
        }

        int sel = select(maxfd + 1, &rfds, NULL, NULL, NULL);
        if (sel < 0) {
            if (errno == EINTR) continue;
            break;
        }

        /* Handle out-of-turn data from other player first (extra-credit) */
        if (other_player->sock >= 0 && FD_ISSET(other_player->sock, &rfds)) {
            message_t imsg;
            int r = read_ngp_message(other_player->sock, &imsg);
            if (r < 0) {
                /* disconnected -> current player wins by forfeit */
                if (other_player->sock >= 0) { close(other_player->sock); other_player->sock = -1; }
                pthread_mutex_lock(&game->lock);
                game->game_over = 1;
                game->winner = cur_turn;
                game->forfeited = 1;
                pthread_mutex_unlock(&game->lock);
                break;
            }
            if (r > 0) {
                /* presentation/protocol error from other -> FAIL and close (opponent wins) */
                send_fail(other_player->sock, r, "Invalid");
                free_message(&imsg);
                if (other_player->sock >= 0) { close(other_player->sock); other_player->sock = -1; }
                pthread_mutex_lock(&game->lock);
                game->game_over = 1;
                game->winner = cur_turn;
                game->forfeited = 0;
                pthread_mutex_unlock(&game->lock);
                break;
            }

            if (imsg.type == MSG_MOVE) {
                /* impatient move: immediate FAIL 31 (do not close) */
                send_fail(other_player->sock, 31, "Impatient");
                free_message(&imsg);
                /* continue: still waiting for current player's move */
            } else {
                /* any other in-game messages are invalid -> FAIL 10 and close (opponent wins) */
                send_fail(other_player->sock, 10, "Invalid");
                free_message(&imsg);
                if (other_player->sock >= 0) { close(other_player->sock); other_player->sock = -1; }
                pthread_mutex_lock(&game->lock);
                game->game_over = 1;
                game->winner = cur_turn;
                game->forfeited = 0;
                pthread_mutex_unlock(&game->lock);
                break;
            }
        }

        /* Handle current player's input */
        if (current_player->sock >= 0 && FD_ISSET(current_player->sock, &rfds)) {
            message_t msg;
            int rr = read_ngp_message(current_player->sock, &msg);
            if (rr < 0) {
                /* disconnected -> other player wins by forfeit */
                nim_logf("Game %d: Player %d disconnected", game->id, cur_turn);
                if (current_player->sock >= 0) { close(current_player->sock); current_player->sock = -1; }
                pthread_mutex_lock(&game->lock);
                game->game_over = 1;
                game->winner = (cur_turn == 1) ? 2 : 1;
                game->forfeited = 1;
                pthread_mutex_unlock(&game->lock);
                break;
            }
            if (rr > 0) {
                /* presentation/protocol error -> FAIL and close -> opponent wins */
                send_fail(current_player->sock, rr, "Invalid");
                free_message(&msg);
                if (current_player->sock >= 0) { close(current_player->sock); current_player->sock = -1; }
                pthread_mutex_lock(&game->lock);
                game->game_over = 1;
                game->winner = (cur_turn == 1) ? 2 : 1;
                game->forfeited = 0;
                pthread_mutex_unlock(&game->lock);
                break;
            }

            if (msg.type != MSG_MOVE) {
                /* sending anything other than MOVE during your turn is invalid -> FAIL 10 and close */
                send_fail(current_player->sock, 10, "Invalid");
                free_message(&msg);
                if (current_player->sock >= 0) { close(current_player->sock); current_player->sock = -1; }
                pthread_mutex_lock(&game->lock);
                game->game_over = 1;
                game->winner = (cur_turn == 1) ? 2 : 1;
                game->forfeited = 0;
                pthread_mutex_unlock(&game->lock);
                break;
            }

            /* Expect exactly 2 fields: pile and qty */
            if (msg.field_count != 2) {
                /* presentation error: FAIL 10 and close -> opponent wins */
                send_fail(current_player->sock, 10, "Invalid");
                free_message(&msg);
                if (current_player->sock >= 0) { close(current_player->sock); current_player->sock = -1; }
                pthread_mutex_lock(&game->lock);
                game->game_over = 1;
                game->winner = (cur_turn == 1) ? 2 : 1;
                game->forfeited = 0;
                pthread_mutex_unlock(&game->lock);
                break;
            }

            char *endptr = NULL;
            errno = 0;
            long pile_val = strtol(msg.fields[0], &endptr, 10);
            if (errno == ERANGE || *endptr != '\0' || pile_val < 1 || pile_val > 5) {
                send_fail(current_player->sock, 32, "Pile Index");
                free_message(&msg);
                continue;
            }
            errno = 0;
            long qty_val = strtol(msg.fields[1], &endptr, 10);
            if (errno == ERANGE || *endptr != '\0' || qty_val < 1 || qty_val > INT_MAX) {
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
                if (valid == 32) send_fail(current_player->sock, 32, "Pile Index");
                else send_fail(current_player->sock, 33, "Quantity");
                free_message(&msg);
                continue;
            }

            /* Apply move */
            game->board[pile] -= qty;
            nim_logf("Game %d: Player %d removed %d from pile %d", game->id, cur_turn, qty, pile + 1);

            if (is_board_empty(game->board)) {
                game->game_over = 1;
                game->winner = cur_turn;
                game->forfeited = 0;
                nim_logf("Game %d: Player %d won", game->id, game->winner);
            } else {
                game->current_turn = (cur_turn == 1) ? 2 : 1;
            }
            pthread_mutex_unlock(&game->lock);

            free_message(&msg);
        }
    } /* end while */

    /* send OVER if game ended */
    if (game->game_over) {
        if (p1->sock >= 0) send_over(p1->sock, game->winner, game->board, game->forfeited);
        if (p2->sock >= 0) send_over(p2->sock, game->winner, game->board, game->forfeited);
    }

    /* cleanup sockets */
    if (p1->sock >= 0) { close(p1->sock); p1->sock = -1; }
    if (p2->sock >= 0) { close(p2->sock); p2->sock = -1; }

    /* remove clients/games from global lists under lock then free */
    pthread_mutex_lock(&global_mutex);
    remove_client_locked(p1);
    remove_client_locked(p2);
    remove_game_locked(game->id);
    pthread_mutex_unlock(&global_mutex);

    free(p1->name);
    free(p2->name);
    free(p1);
    free(p2);

    pthread_mutex_destroy(&game->lock);
    free(game);

    return NULL;
}

/* -------------------------------------------------------------------------- */
/* Matchmaking: add client to global list (under lock) and match if possible.
   This function keeps global list operations atomic to avoid races. */
void add_wait(client_t *c) {
    if (!c) return;

    pthread_mutex_lock(&global_mutex);

    /* ensure capacity and add client */
    ensure_client_capacity_locked();
    if (client_count == client_cap) {
        /* fatal: cannot add, clean up */
        pthread_mutex_unlock(&global_mutex);
        send_fail(c->sock, 10, "Invalid");
        close(c->sock);
        free(c->name);
        free(c);
        return;
    }
    clients[client_count++] = c;

    /* search for waiting opponent */
    client_t *opponent = NULL;
    for (int i = 0; i < client_count; ++i) {
        if (clients[i] != c && clients[i]->is_waiting && !clients[i]->is_playing) {
            opponent = clients[i];
            break;
        }
    }

    if (!opponent) {
        /* no opponent yet: mark waiting and release lock */
        c->is_waiting = 1;
        pthread_mutex_unlock(&global_mutex);
        send_wait(c->sock);
        return;
    }

    /* Found opponent: mark both as playing */
    opponent->is_waiting = 0;
    opponent->is_playing = 1;
    c->is_waiting = 0;
    c->is_playing = 1;

    /* create and add game under lock */
    game_t *game = calloc(1, sizeof(game_t));
    if (!game) {
        /* cleanup: remove both clients and disconnect */
        remove_client_locked(opponent);
        remove_client_locked(c);
        pthread_mutex_unlock(&global_mutex);
        send_fail(c->sock, 10, "Invalid");
        send_fail(opponent->sock, 10, "Invalid");
        close(c->sock);
        close(opponent->sock);
        free(c->name); free(opponent->name);
        free(c); free(opponent);
        return;
    }

    game->player1 = c;
    game->player2 = opponent;
    game->board[0] = 1;
    game->board[1] = 3;
    game->board[2] = 5;
    game->board[3] = 7;
    game->board[4] = 9;
    pthread_mutex_init(&game->lock, NULL);

    /* assign id and add game to global list */
    game->id = next_game_id++;
    add_game_locked(game); /* adds under the same global_mutex */
    pthread_mutex_unlock(&global_mutex);

    /* finalize game and clients */
    game->current_turn = 1;
    game->game_over = 0;
    game->winner = 0;
    game->forfeited = 0;
    c->game_id = game->id;
    opponent->game_id = game->id;
    c->player_num = 1;
    opponent->player_num = 2;

    /* spawn game thread */
    pthread_t tid;
    if (pthread_create(&tid, NULL, game_thread, game) != 0) {
        perror("pthread_create failed");
        /* try to clean up: remove game from global list */
        pthread_mutex_lock(&global_mutex);
        remove_game_locked(game->id);
        pthread_mutex_unlock(&global_mutex);
        pthread_mutex_destroy(&game->lock);
        free(game);
        return;
    }
    pthread_detach(tid);
}

/* -------------------------------------------------------------------------- */
/* Handle newly accepted client: read OPEN, validate, then add_wait() */
void handle_client(int sock) {
    message_t msg;
    int msg_result = read_ngp_message(sock, &msg);

    if (msg_result < 0) {
        close(sock);
        return;
    }
    if (msg_result > 0) {
        /* framing/protocol error on first message -> FAIL and close */
        send_fail(sock, msg_result, "Invalid");
        close(sock);
        return;
    }

    /* First message must be OPEN */
    if (msg.type == MSG_MOVE) {
        send_fail(sock, 24, "Not Playing");
        free_message(&msg);
        close(sock);
        return;
    }
    if (msg.type != MSG_OPEN) {
        send_fail(sock, 10, "Invalid");
        free_message(&msg);
        close(sock);
        return;
    }

    /* Expect exactly 1 field: player name */
    if (msg.field_count != 1) {
        send_fail(sock, 10, "Invalid");
        free_message(&msg);
        close(sock);
        return;
    }

    const char *name = msg.fields[0];
    int name_valid = validate_name_field(name);
    if (name_valid != 0) {
        if (name_valid == 21) send_fail(sock, 21, "Long Name");
        else send_fail(sock, 10, "Invalid");
        free_message(&msg);
        close(sock);
        return;
    }

    /* Name already in use by a playing client -> FAIL 22 */
    if (is_name_in_use(name)) {
        send_fail(sock, 22, "Already Playing");
        free_message(&msg);
        close(sock);
        return;
    }

    /* allocate client structure */
    client_t *c = calloc(1, sizeof(client_t));
    if (!c) {
        send_fail(sock, 10, "Invalid");
        free_message(&msg);
        close(sock);
        return;
    }
    c->sock = sock;
    c->name = strdup(name);
    c->game_id = -1;
    c->player_num = 0;
    c->is_waiting = 0;
    c->is_playing = 0;

    free_message(&msg);

    /* Check for immediate second message (non-blocking). If client
       sends OPEN again -> FAIL 23. If client sends MOVE before NAME -> FAIL 24. */
    fd_set rfds;
    struct timeval tv = {0, 0};
    FD_ZERO(&rfds);
    FD_SET(sock, &rfds);
    int r = select(sock + 1, &rfds, NULL, NULL, &tv);
    if (r > 0 && FD_ISSET(sock, &rfds)) {
        message_t msg2;
        int r2 = read_ngp_message(sock, &msg2);
        if (r2 < 0) {
            /* disconnected while setting up */
            free(c->name);
            free(c);
            close(sock);
            return;
        }
        if (r2 > 0) {
            /* protocol error on second message - close */
            send_fail(sock, r2, "Invalid");
            free_message(&msg2);
            free(c->name);
            free(c);
            close(sock);
            return;
        }
        if (msg2.type == MSG_OPEN) {
            send_fail(sock, 23, "Already Open");
            free_message(&msg2);
            free(c->name);
            free(c);
            close(sock);
            return;
        } else if (msg2.type == MSG_MOVE) {
            send_fail(sock, 24, "Not Playing");
            free_message(&msg2);
            free(c->name);
            free(c);
            close(sock);
            return;
        } else {
            send_fail(sock, 10, "Invalid");
            free_message(&msg2);
            free(c->name);
            free(c);
            close(sock);
            return;
        }
    }

    nim_logf("Player %s connected", c->name);
    add_wait(c);
}

/* Client thread wrapper: accept malloc'd int* argument */
void *client_thread(void *arg) {
    if (!arg) return NULL;
    int sock = *((int *)arg);
    free(arg);
    handle_client(sock);
    return NULL;
}

/* -------------------------------------------------------------------------- */
/* Main: listen and spawn client threads, shutdown cleanly */

int main(int argc, char **argv) {
    char *service = "15000";
    if (argc >= 2) service = argv[1];

    install_handlers();

    int listener = open_listener(service, QUEUE);
    if (listener < 0) exit(EXIT_FAILURE);

    nim_logf("Nim server listening on port %s", service);

    while (active) {
        struct sockaddr_storage remote_host;
        socklen_t remote_host_len = sizeof(remote_host);
        int sock = accept(listener, (struct sockaddr*)&remote_host, &remote_host_len);
        if (sock < 0) {
            if (active) perror("accept failed");
            continue;
        }

        pthread_t tid;
        int *psock = malloc(sizeof(int));
        if (!psock) {
            perror("malloc failed");
            close(sock);
            continue;
        }
        *psock = sock;
        if (pthread_create(&tid, NULL, client_thread, psock) != 0) {
            perror("pthread_create failed");
            close(sock);
            free(psock);
            continue;
        }
        pthread_detach(tid);
    }

    close(listener);

    /* Shutdown: close remaining clients and free resources */
    pthread_mutex_lock(&global_mutex);
    for (int i = 0; i < client_count; ++i) {
        if (clients[i]) {
            close(clients[i]->sock);
            free(clients[i]->name);
            free(clients[i]);
        }
    }
    free(clients);
    clients = NULL;
    client_count = 0;

    for (int i = 0; i < game_count; ++i) {
        if (games[i]) {
            pthread_mutex_destroy(&games[i]->lock);
            free(games[i]);
        }
    }
    free(games);
    games = NULL;
    game_count = 0;
    pthread_mutex_unlock(&global_mutex);

    return EXIT_SUCCESS;
}
