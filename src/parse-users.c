#include <errno.h>

char * name = NULL;
size_t name_bufsize = 0;
unsigned long name_len;
unsigned long long score;
unsigned int units;
unsigned int team;
MYSQL_BIND next_params[4];
MYSQL_STMT * set_next_stmt;
MYSQL_STMT * find_next_stmt;
MYSQL_STMT * insert_stmt;

void userSetup() {
	if(mysql_query(dbconn,
"CREATE TEMPORARY TABLE `user_temp` ( \
  `id` int(10) unsigned, \
  `name` varchar(255) NOT NULL, \
  `score` bigint(20) unsigned NOT NULL, \
  `units` int(10) unsigned NOT NULL, \
  `team` int(10) unsigned NOT NULL, \
  UNIQUE KEY `id` (`id`), \
  KEY `team` (`team`) \
) ENGINE=MEMORY DEFAULT CHARSET=latin1")) {
		mysqlErrAndExit(dbconn, "CREATE");
	}

	char * set_next_stmt_sql = 
"SET @name=?, @score=?, @units=?, @team=?";

	set_next_stmt = mysql_stmt_init(dbconn);
	if(!set_next_stmt) {
		mysqlErrAndExit(dbconn, "mysql_stmt_init");
	}

	if(mysql_stmt_prepare(set_next_stmt, set_next_stmt_sql, strlen(set_next_stmt_sql))) {
		mysqlErrAndExit(dbconn, "mysql_stmt_prepare set_next_user");
	}

// set next_user to the highest scoring existing user we haven't seen yet or NULL if one doesn't exist
	char * find_next_stmt_sql = 
"SET @next_user = \
(SELECT user.id FROM user \
LEFT JOIN user_temp ON user.id = user_temp.id \
WHERE user.name = @name AND user.team = @team AND \
user_temp.id IS NULL \
ORDER BY user.score DESC LIMIT 1)";

	find_next_stmt = mysql_stmt_init(dbconn);
	if(!find_next_stmt) {
		mysqlErrAndExit(dbconn, "mysql_stmt_init");
	}

	if(mysql_stmt_prepare(find_next_stmt, find_next_stmt_sql, strlen(find_next_stmt_sql))) {
		mysqlErrAndExit(dbconn, "mysql_stmt_prepare find_next_user");
	}

	 char * insert_stmt_sql = 
"INSERT INTO user_temp (id, name, score, units, team) \
VALUES (@next_user, @name, @score, @units, @team)";


	insert_stmt = mysql_stmt_init(dbconn);
	if(!insert_stmt) {
		mysqlErrAndExit(dbconn, "mysql_stmt_init");
	}

	if(mysql_stmt_prepare(insert_stmt, insert_stmt_sql, strlen(insert_stmt_sql))) {
		mysqlErrAndExit(dbconn, "mysql_stmt_prepare insert_user");
	}

	memset(next_params, 0, sizeof(next_params));

	next_params[0].buffer_type = MYSQL_TYPE_STRING;
	next_params[0].length = &name_len;

	next_params[1].buffer_type = MYSQL_TYPE_LONGLONG;
	next_params[1].buffer = &score;
	next_params[1].buffer_length = sizeof(score);
	next_params[1].is_unsigned = 1;

	next_params[2].buffer_type = MYSQL_TYPE_LONG;
	next_params[2].buffer = &units;
	next_params[2].buffer_length = sizeof(units);
	next_params[2].is_unsigned = 1;

	next_params[3].buffer_type = MYSQL_TYPE_LONG;
	next_params[3].buffer = &team;
	next_params[3].buffer_length = sizeof(team);
	next_params[3].is_unsigned = 1;
}


void parseUserTokens(char * name_tok, char * score_tok, char * units_tok, char * team_tok) {

	if(name_bufsize != bufsize) {
		name = realloc(name, bufsize);
		if(name == NULL) {
			perror("malloc");
			exit(1);
		}
		name_bufsize = bufsize;
		next_params[0].buffer = name;
		next_params[0].buffer_length = name_bufsize;
		mysql_stmt_bind_param(set_next_stmt, next_params);
	}

	strcpy(name, name_tok);
	name_len = strlen(name);

	errno = 0;

	if(strcmp(score_tok, "NULL") == 0) {
		score = 0;
	} else {
		score = strtoll(score_tok, NULL, 10);
		if(errno) {
			errAndExit("Bad user score token");
		}
	}

	units = strtol(units_tok, NULL, 10);
	if(errno) {
		errAndExit("Bad user units token");
	}

	team = strtol(team_tok, NULL, 10);
	if(errno) {
		errAndExit("Back user team token");
	}


	if(mysql_stmt_execute(set_next_stmt)) {
		mysqlErrAndExit(dbconn, "(prepared) set_next_stmt");
	}

	if(mysql_stmt_execute(find_next_stmt)) {
		mysqlErrAndExit(dbconn, "(prepared) find_next_stmt");
	}

	if(mysql_stmt_execute(insert_stmt)) {
		mysqlErrAndExit(dbconn, "(prepared) insert_stmt");
	}

}


void userTeardown() {
	mysql_stmt_close(find_next_stmt);
	mysql_stmt_close(set_next_stmt);
	mysql_stmt_close(insert_stmt);
}

void updateUserSourcedTeams() {
	long long int rows;

	printf("Inserting/Updating user-sourced teams... ");
	fflush(stdout);

	startTimer();
	if(mysql_query(dbconn, 
"INSERT INTO team (id, score, units) \
  SELECT user_temp.team, SUM(score), SUM(units) \
  FROM user_temp \
  WHERE user_temp.team NOT IN (SELECT team_temp.id FROM team_temp) \
  GROUP BY team \
ON DUPLICATE KEY UPDATE \
score=VALUES(score), \
units=VALUES(units)")) {
		mysqlErrAndExit(dbconn, "INSERT INTO team");
	}
	saveTime("user_teams_update_time");

	rows = mysql_affected_rows(dbconn);
	setUpdateInt("user_teams_updated", rows);
	printf("affected %lld teams\n", rows);

}

void updateUsers() {
	updateUserSourcedTeams();

	long long int rows;

	printf("Deleting unseen users... ");
	fflush(stdout);

	startTimer();
	if(mysql_query(dbconn,
"DELETE FROM user \
WHERE id NOT IN (SELECT id FROM user_temp)")) {
		mysqlErrAndExit(dbconn, "DELETE");
	}
	saveTime("users_delete_time");

	rows = mysql_affected_rows(dbconn);
	setUpdateInt("users_deleted", rows);
	printf("deleted %lld old users\n", rows);

	printf("Inserting/Updating seen users... ");
	fflush(stdout);

	startTimer();
	if(mysql_query(dbconn,
"INSERT INTO user (id, name, score, units, team) \
SELECT id, name, score, units, team FROM user_temp \
ON DUPLICATE KEY UPDATE \
score=VALUES(score), \
units=VALUES(units)")) {
		mysqlErrAndExit(dbconn, "INSERT INTO user");
	}
	saveTime("users_update_time");

	rows = mysql_affected_rows(dbconn);
	setUpdateInt("users_updated", rows);	
	printf("affected %lld users\n", rows);


	printf("Deleting unseen user-sourced teams... ");
	fflush(stdout);

	startTimer();
	if(mysql_query(dbconn,
"DELETE FROM team WHERE \
name IS NULL AND \
NOT EXISTS (SELECT NULL FROM user_temp WHERE team=team.id)")) {
		mysqlErrAndExit(dbconn, "DELETE");
	}
	saveTime("user_teams_delete_time");

	rows = mysql_affected_rows(dbconn);
	setUpdateInt("user_teams_deleted", rows);
	printf("deleted %lld old user-sourced teams\n", rows);

}
