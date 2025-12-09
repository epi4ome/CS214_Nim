#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <poll.h>
#include "network.h"
#include "pbuf.h"

#define BUFLEN 256

int
main (int argc, char **argv)
{
    if (argc < 3) {
	printf ("Usage: %s host port\n", argv[0]);
	exit (EXIT_FAILURE);
    }

    int sock = connect_inet (argv[1], argv[2]);
    if (sock < 0) exit (EXIT_FAILURE);

    struct pollfd pfds[2];
    pfds[0].fd     = STDIN_FILENO;
    pfds[0].events = POLLIN;
    pfds[1].fd     = sock;
    pfds[1].events = POLLIN;

    char buf[BUFLEN];
    int bytes;

    for (;;) {
	int ready = poll (pfds, 2, -1);
	if (ready < 0) {
	    perror("poll");
	    break;
	}

	if (pfds[0].revents) {
	    bytes = read (STDIN_FILENO, buf, BUFLEN);

	    if (bytes < 1) {
		printf("Exiting\n");
		break;
	    }

	    if (buf[bytes-1] == '\n') {
		bytes--;
	    }

	    printf ("Sending %d bytes\n", bytes);
	    write (sock, buf, bytes);
	}
	
	if (pfds[1].revents) {
	    bytes = read (sock, buf, BUFLEN);

	    if (bytes < 1) {
		printf ("Socket EOF or error\n");
		break;
	    }

	    printf ("Recv %3d [", bytes);
	    print_buffer (buf, bytes);
	    printf ("]\n");

	}

    }

    close (sock);

    return EXIT_SUCCESS;
}
