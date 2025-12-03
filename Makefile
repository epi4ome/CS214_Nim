all: nimd

nimd: nimd.c
	gcc -Wall -Wextra -pthread -g -o nimd nimd.c

clean:
	rm -f nimd