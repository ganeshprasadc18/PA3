CC=gcc
CFLAGS=-Wall

all: mysort

mysort: mysort.c
	$(CC) $(CFLAGS) -o $@ mysort.c -g

clean:
	rm -rf mysort
