#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql/mysql.h>
#include "../config.h"

void mysqlErrAndExit(MYSQL* dbconn, char *text) {
        printf("MySQL Error, %s: %u: %s\n", text, mysql_errno(dbconn), mysql_error(dbconn));
        mysql_close(dbconn);
        exit(1);
}

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



int main(int argc, char *argv[]) {
	if(argc < 2) {
		fprintf(stderr, "usage: %s <log-file>\n", argv[0]);
		exit(1);
	}

	FILE *fp = fopen(argv[1], "rb");
	if(fp == NULL) {
		fprintf(stderr, "Could not open file\n");
		exit(1);
	}

	MYSQL* dbconn;
	dbconn = getMysqlConn();

	// file read: http://stackoverflow.com/questions/2029103/correct-way-to-read-a-text-file-into-a-buffer-in-c
	   /* Go to the end of the file. */
	if (fseek(fp, 0L, SEEK_END) != 0) {
		fprintf(stderr, "Seek (to end) error\n");
		exit(1);
	}

	/* Get the size of the file. */
	long bufsize = ftell(fp);
	if (bufsize == -1) { 
		fprintf(stderr, "ftell error\n");
		exit(1);
	}

	/* Allocate our buffer to that size. */
	char* buffer = malloc(sizeof(char) * bufsize);
	if(buffer == NULL) {
		fprintf(stderr, "malloc error\n");
		exit(1);
	}

	/* Go back to the start of the file. */
	if (fseek(fp, 0L, SEEK_SET) != 0) {
		fprintf(stderr, "Seek (to start) error\n");
		exit(1);
	}

	/* Read the entire file into memory. */
	size_t newLen = fread(buffer, sizeof(char), bufsize, fp);
	if (newLen == 0) {
		fputs("Error reading file", stderr);
		exit(1);
	}

	fclose(fp);

	char * load_file_stmt_sql = 
"UPDATE updates SET output=? ORDER BY id DESC LIMIT 1";

	MYSQL_STMT * load_file_stmt = mysql_stmt_init(dbconn);
	if(!load_file_stmt) {
		mysqlErrAndExit(dbconn, "mysql_stmt_init");
	}

	if(mysql_stmt_prepare(load_file_stmt, load_file_stmt_sql, strlen(load_file_stmt_sql))) {
		mysqlErrAndExit(dbconn, "mysql_stmt_prepare set_next_user");
	}

	MYSQL_BIND load_file_params[1];
	memset(load_file_params, 0, sizeof(load_file_params));

	load_file_params[0].buffer_type = MYSQL_TYPE_BLOB;
	load_file_params[0].buffer = buffer;
	load_file_params[0].buffer_length = bufsize;
	mysql_stmt_bind_param(load_file_stmt, load_file_params);

	if(mysql_stmt_execute(load_file_stmt)) {
		mysqlErrAndExit(dbconn, "(prepared) load_file_stmt");
	}

	mysql_stmt_close(load_file_stmt);

	free(buffer);
	mysql_close(dbconn);
}
