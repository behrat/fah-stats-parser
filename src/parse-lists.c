#include <stdio.h>
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include <mysql/mysql.h>

MYSQL* dbconn;
char * buffer = NULL;
size_t bufsize = 0;
int linelen;

#include "parse-logger.c"
#include "parse-utils.c"
#include "parse-date.c"
#include "parse-teams.c"
#include "parse-users.c"

void getLine() {
	if((linelen = getline(&buffer, &bufsize, stdin)) == -1) {
		errAndExit("Unexpected end-of-file\n");
	}
}

void connectToDB() {
	dbconn = getMysqlConn();

// Allow up to 1G of memory use for temp tables
	if(mysql_query(dbconn,
"SET max_heap_table_size = 1024*1024*1024")) {
		mysqlErrAndExit(dbconn, "CREATE");
	}

}

void parseTeams() {
	struct tm timestamp;

	getLine();
	memset(&timestamp, 0, sizeof(struct tm));
	char* rv = parseDate(buffer, &timestamp);
	if(rv == NULL || *rv != '\0') {
		printf("buffer: %s\n", buffer);
		errAndExit("Expected team file timestamp\n");
	}
	setUpdateTime("teams_timestamp", &timestamp);

	getLine();
	if(strcmp(buffer, "team\tteamname\tscore\twu\n") != 0) {
		errAndExit("Expected team file header\n");
	}
	
	teamSetup();

	printf("Parsing teams");
	fflush(stdout);

	int line = 0;
	while((linelen = getline(&buffer, &bufsize, stdin)) != -1) {
		if(line++ % 1000 == 0) {
			printf(".");
			fflush(stdout);
		}

		if(strchr(buffer, '\t') == NULL) {
			break;
		}

		parseTeamLine();
	}

	printf("\nparsed %d lines.\n", line);
	setUpdateInt("teams_count", line);
}

void parseUsers() {
	struct tm timestamp;

	memset(&timestamp, 0, sizeof(struct tm));
	char* rv = parseDate(buffer, &timestamp);
	if(rv == NULL || *rv != '\0') {
		printf("buffer: %s\n", buffer);
		errAndExit("Expected user file timestamp\n");
	}
	setUpdateTime("users_timestamp", &timestamp);

	getLine();
	if(strcmp(buffer, "name\tnewcredit\tsum(total)\tteam\n") != 0) {
		errAndExit("Expected user file header\n");
	}

	userSetup();

	printf("Parsing users");
	fflush(stdout);

	int line = 0;
	while((linelen = getline(&buffer, &bufsize, stdin)) != -1) {
		if(line++ % 1000 == 0) {
			printf(".");
			fflush(stdout);
		}

		char * tokens[4];
		tokens[0] = strtok(buffer, "\t\n");
		for(int i = 1; i<4; i++) {
			tokens[i] = strtok(NULL, "\t\n");
		}

		if(tokens[3] != NULL) {
			parseUserTokens(tokens[0], tokens[1], tokens[2], tokens[3]);
		} else if (tokens[2] != NULL) {
			parseUserTokens("", tokens[0], tokens[1], tokens[2]);
		} else {
			errAndExit("Unexpected line\n");
		}

	}
	userTeardown();

	printf("\nparsed %d lines.\n", line);
	setUpdateInt("users_count", line);
}

int main(int argc, char ** argv) {
	if(argc > 1) {
		printf("Usage: bzcat daily_team_summary.txt.bz2 daily_user_summary.txt.bz2 | %s\n", argv[0]);
		exit(1);
	}

	connectToDB();
	startUpdate();
	startTimer();
	parseTeams();
	saveTime("teams_parsing_time");

	startTimer();
	parseUsers();
	saveTime("users_parsing_time");

	if(mysql_query(dbconn, "START TRANSACTION")) {
		mysqlErrAndExit(dbconn, "START TRANSACTION");
	}

	updateTeams();
	updateUsers();

	startTimer();
	if(mysql_query(dbconn, "COMMIT")) {
		mysqlErrAndExit(dbconn, "COMMIT");
	}
	saveTime("commit_time");

	if(mysql_query(dbconn, "DROP TABLE `user_temp`")) {
		mysqlErrAndExit(dbconn, "COMMIT");
	}

	if(mysql_query(dbconn, "DROP TABLE `team_temp`")) {
		mysqlErrAndExit(dbconn, "COMMIT");
	}

	finishUpdate();
	mysql_close(dbconn);
	return 0;
}

