CC = gcc
CFLAGS = -g -Wall -std=c99 -fsanitize=address,undefined

rawc: rawc.o pbuf.o network.o
	$(CC) $(CFLAGS) -o $@ $^

clean:
	rm -f rawc *.o
