#include "../config.h"
#include <stdarg.h>

MYSQL* getMysqlConn() {
	MYSQL *dbconn = mysql_init(NULL);

	if(dbconn == NULL) {
		mysqlErrAndExit(dbconn, "init");
	}

	printf("Connecting to database... ");
	dbconn = mysql_real_connect(dbconn, MYSQL_HOST, MYSQL_USER, MYSQL_PASS, MYSQL_DB, 0, NULL, 0);
	if(dbconn == NULL) {
		printf("[ FAIL ]\n");
		mysqlErrAndExit(dbconn, "connect");
	}
	printf("[ OK ]\n");

	return dbconn;
}

int bprintf(char ** buffer, size_t * bufsize, const char * format, ... ) {
	va_list args;
	va_start(args, format);
	int len = vsnprintf(*buffer, *bufsize, format, args);
	va_end(args);
	if(len >= *bufsize) {
		*buffer = realloc(*buffer, len+1);
		if(*buffer == NULL) {
			*bufsize = 0;
		} else {
			*bufsize = len+1;
			va_list args;
			va_start(args, format);
			len = vsnprintf(*buffer, *bufsize, format, args);
			va_end(args);
		}
	}

	return len;
}


void mysqlErrAndExit(MYSQL* dbconn, char *text) {
	printf("MySQL Error, %s: %u: %s\n", text, mysql_errno(dbconn), mysql_error(dbconn));
	failUpdate();
	mysql_close(dbconn);
	exit(1);
}

void errAndExit(char *text) {
	printf("Error: %s\n", text);
	failUpdate();
	mysql_close(dbconn);
	exit(1);
}
