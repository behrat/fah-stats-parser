#define __USE_XOPEN
#include <time.h>
#include <sys/time.h>
#include <string.h>

char * parseDate(char* buffer, struct tm * time) {
        char* timezone;
        char* timezones[] = {"PST", "PDT"};
        int timezone_count = sizeof(timezones) / sizeof(*timezones);

        for(int i = 0;  i<timezone_count && !timezone; i++) {
            timezone = strstr(buffer, timezones[i]);
        }

        if(!timezone) {
            fprintf(stderr, "Couldn't parse date (timezone not found)\n");
            return NULL;
        }
        

	memcpy(timezone, "   ", 3); // relace timezone with spaces
	return strptime(buffer, "%A%t%B %d %T     %Y\n", time);
}
