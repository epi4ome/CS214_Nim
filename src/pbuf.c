#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "pbuf.h"

#ifdef NOCOLOR
#define NORMAL ""
#define CYAN ""
#define RED ""
#else
#define NORMAL "\x1b[0m"
#define CYAN "\x1b[1;36m"
#define RED "\x1b[1;31m"
#endif


void
print_buffer(char *buf, unsigned len)
{
    for (int i = 0; i < len; i++) {
	unsigned char c = buf[i];
	if (c < 32) {
	    printf(CYAN "^%c" NORMAL, c + 64);
	} else if (c == 128) {
	    printf(CYAN "^?" NORMAL);
	} else if (c > 128) {
	    printf(RED "<%X>" NORMAL, c);
	} else {
	    putchar(c);
	}
    }
}
