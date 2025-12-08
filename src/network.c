#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netdb.h>
#include <string.h>
#include "network.h"

int connect_inet(char *host, char *service)
{
    struct addrinfo hints, *info_list, *info;
    int sock, error;

    // look up remote host
    memset(&hints, 0, sizeof(hints));
    hints.ai_family   = AF_UNSPEC;  // in practice, this means give us IPv4 or IPv6
    hints.ai_socktype = SOCK_STREAM; // indicate we want a streaming socket

    error = getaddrinfo(host, service, &hints, &info_list);
    if (error) {
        fprintf(stderr, "error looking up %s:%s: %s\n", host, service, gai_strerror(error));
        return -1;
    }

    for (info = info_list; info != NULL; info = info->ai_next) {
        sock = socket(info->ai_family, info->ai_socktype, info->ai_protocol);
        if (sock < 0) continue;

        error = connect(sock, info->ai_addr, info->ai_addrlen);
        if (error) {
            close(sock);
            continue;
        }

        break;
    }
    freeaddrinfo(info_list);

    if (info == NULL) {
        fprintf(stderr, "Unable to connect to %s:%s\n", host, service);
        return -1;
    }

    return sock;
}

int open_listener(char *service, int queue_size)
{
    struct addrinfo hint, *info_list, *info;
    int error, sock;

    // initialize hints
    memset(&hint, 0, sizeof(struct addrinfo));
    hint.ai_family   = AF_UNSPEC;
    hint.ai_socktype = SOCK_STREAM;
    hint.ai_flags    = AI_PASSIVE;

    // obtain information for listening socket
    error = getaddrinfo(NULL, service, &hint, &info_list);
    if (error) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(error));
        return -1;
    }

    // attempt to create socket
    for (info = info_list; info != NULL; info = info->ai_next) {
        sock = socket(info->ai_family, info->ai_socktype, info->ai_protocol);

        // if we could not create the socket, try the next method
        if (sock == -1) continue;

        // bind socket to requested port
        error = bind(sock, info->ai_addr, info->ai_addrlen);
        if (error) {
            close(sock);
            continue;
        }

        // enable listening for incoming connection requests
        error = listen(sock, queue_size);
        if (error) {
            close(sock);
            continue;
        }

        // if we got this far, we have opened the socket
        break;
    }

    freeaddrinfo(info_list);

    // info will be NULL if no method succeeded
    if (info == NULL) {
        fprintf(stderr, "Could not bind\n");
        return -1;
    }

    return sock;
}
