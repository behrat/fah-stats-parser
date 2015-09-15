#include <time.h>
#include <mysql/mysql.h>

#include "parse-utils.h"

MYSQL* logconn;

struct timespec start_time;
struct timespec end_time;

char * log_buffer = NULL;
size_t log_bufsize = 0;

void startUpdate() {
	logconn = getMysqlConn();

	if(mysql_query(logconn, "INSERT INTO updates (status, start_timestamp) VALUES ('STARTED', NOW())")) {
		mysqlErrAndExit(logconn, "update metadata");
	}

	if(mysql_query(logconn, "SET @updateid = LAST_INSERT_ID()")) {
		mysqlErrAndExit(logconn, "update metadata");
	}
}

void failUpdate() {
	if(mysql_query(logconn, "UPDATE updates SET status='FAILED', end_timestamp=NOW() WHERE id = @updateid")) {
		mysqlErrAndExit(logconn, "update metadata");
	}
}

void finishUpdate() {
	if(mysql_query(logconn, "UPDATE updates SET status='FINISHED', end_timestamp=NOW()  WHERE id = @updateid")) {
		mysqlErrAndExit(logconn, "update metadata");
	}
}

void setUpdateInt(char * key, long long int value) {
	bprintf(&log_buffer, &log_bufsize, "UPDATE updates SET %s=%lld WHERE id = @updateid", key, value);
	if(mysql_query(logconn, log_buffer)) {
		mysqlErrAndExit(logconn, "update metadata");
	}
}

void setUpdateTime(char * key, const struct tm *restrict timeptr) {
	char datetime[22];
	if(strftime(datetime, 22, "%F %T", timeptr) == 0) {
		errAndExit("datetime string overflow");
	}

	bprintf(&log_buffer, &log_bufsize, "UPDATE updates SET %s='%s' WHERE id = @updateid", key, datetime);

	if(mysql_query(logconn, log_buffer)) {
		mysqlErrAndExit(logconn, "update metadata");
	}
}

void startTimer() {
	clock_gettime(CLOCK_MONOTONIC, &start_time);
}

double saveTime(char * key) {
	clock_gettime(CLOCK_MONOTONIC, &end_time);
	double elapsed_time = (end_time.tv_sec - start_time.tv_sec) + (end_time.tv_nsec - start_time.tv_nsec)/((double) 1000000000);
	bprintf(&log_buffer, &log_bufsize, "UPDATE updates SET %s=%f WHERE id = @updateid", key, elapsed_time);

	if(mysql_query(logconn, log_buffer)) {
		mysqlErrAndExit(logconn, "update metadata");
	}

	return elapsed_time;
}
