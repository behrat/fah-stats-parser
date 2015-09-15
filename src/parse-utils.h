#ifndef FAH_PARSE_UTILITIES_H
#define FAH_PARSE_UTILITIES_H

MYSQL* getMysqlConn();
int bprintf(char ** buffer, size_t * bufsize, const char * format, ... );
void mysqlErrAndExit(MYSQL* dbconn, char *text);
void errAndExit(char *text);

#endif
