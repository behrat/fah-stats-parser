
void teamSetup() {
	if(mysql_query(dbconn,
"CREATE TEMPORARY TABLE `team_temp` ( \
  `id` int(10) unsigned, \
  `name` varchar(255) NOT NULL, \
  `score` bigint(20) unsigned NOT NULL, \
  `units` int(10) unsigned NOT NULL, \
  PRIMARY KEY `id` (`id`) \
) ENGINE=MEMORY DEFAULT CHARSET=latin1")) {
		mysqlErrAndExit(dbconn, "CREATE");
	}
}

char * team_name_raw = NULL;
char * team_name = NULL;

void parseTeamLine() {
	unsigned int id;
	unsigned long score;
	unsigned int units;

	team_name_raw = realloc(team_name_raw, bufsize);
	team_name = realloc(team_name, 2*bufsize);

	int items = sscanf(buffer, "%u\t%[^\t]\t%lu\t%u\n", &id, team_name_raw, &score, &units);

	if(items == 3) {
		strcpy(team_name_raw, "");
		items = sscanf(buffer, "%u\t\t%lu\t%u\n", &id, &score, &units);
	} else if (items != 4) {
		printf("Could not parse line: %s\n", buffer);
		errAndExit("parseTeamLine()");
	}

	mysql_real_escape_string(dbconn, team_name, team_name_raw, strlen(team_name_raw));

	bprintf(&buffer, &bufsize,
"INSERT INTO team_temp (id, name, score, units) \
VALUES (%u,'%s',%lu, %u)", id, team_name, score, units);

	if(mysql_query(dbconn, buffer)) {
		mysqlErrAndExit(dbconn, "INSERT");
	}	
}

void updateTeams() {
	long long int rows;

	printf("Nulling unseen team names... ");
	fflush(stdout);

	startTimer();
	if(mysql_query(dbconn,
"UPDATE team SET name=NULL WHERE \
name IS NOT NULL AND \
NOT EXISTS (SELECT NULL FROM team_temp WHERE team.id = team_temp.id)")) {
		mysqlErrAndExit(dbconn, "DELETE");
	}
	saveTime("teams_delete_time");

	rows = mysql_affected_rows(dbconn);
	setUpdateInt("teams_deleted", rows);
	printf("deleted %llu old teams\n", rows);

	printf("Inserting/Updating seen teams... ");
	fflush(stdout);

	startTimer();
	if(mysql_query(dbconn,
"INSERT INTO team (id, name, score, units) \
  SELECT id, name, score, units FROM team_temp \
ON DUPLICATE KEY UPDATE \
name=VALUES(name), \
score=VALUES(score), \
units=VALUES(units)")) {
		mysqlErrAndExit(dbconn, "INSERT INTO team");
	}
	saveTime("teams_update_time");

	rows = mysql_affected_rows(dbconn);
	setUpdateInt("teams_updated", rows);
	printf("affected %llu teams\n", rows);
}
