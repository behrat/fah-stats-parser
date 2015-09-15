#include <stdio.h>
#include <stdlib.h>

#include "parse-date.c"

#define OUTPUT_FORMAT "%F_%T"
#define OUTPUT_BUFSIZE 20

int main(int argc, char ** argv) {
	if(argc < 2) {
		printf("Usage: %s <fah summary timestamp>\n", argv[0]);
		exit(1);
	}

	struct tm timestamp;

	char* rv = parseDate(argv[1], &timestamp);
	if(rv == NULL || *rv != '\0') {
		printf("Couldn't read timestamp\n");
		exit(1);
	}

	char buffer[OUTPUT_BUFSIZE];

	if(strftime(buffer, OUTPUT_BUFSIZE, OUTPUT_FORMAT, &timestamp) == 0) {
		printf("Couldn't write timestamp\n");
		exit(1);
	}

	printf("%s\n", buffer);
	return 0;
}
