package com.datastax.sample.dao;

import java.util.List;
import java.util.Set;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.EC2MultiRegionAddressTranslater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;

public class SampleDao {

	private static Logger logger = LoggerFactory.getLogger(SampleDao.class);
	private Session session;

	private static String keyspaceName = "sample_keyspace";
	private static String testTable = keyspaceName + ".test";
	private List<KeyspaceMetadata> keyspaces;
	
	public SampleDao(String[] contactPoints) {

		Cluster cluster = Cluster.builder()
				.addContactPoints(contactPoints)
				.withAddressTranslater(new EC2MultiRegionAddressTranslater())
				.build();

		this.session = cluster.connect();
		this.keyspaces = cluster.getMetadata().getKeyspaces();
	}

	public List<KeyspaceMetadata> getKeyspaces() {
		return keyspaces;
	}

	void run(String cql){
		logger.info("Running : " + cql);
		session.execute(cql);
	}

	public boolean runLWT(String cql) {
		logger.info("Running : " + cql);
		ResultSet rset = session.execute(cql);
		Row row = rset.one(); // if this is true then the statement was successful
		boolean lwtSucess = row.getBool(0);       // this is equivalent row.getBool("applied")
		logger.info("lwt result : " + lwtSucess);
		return lwtSucess;
	}

	public void insertPlayers() {
		run("insert into lwt_example_db.players (player_id, name, assigned, stats) values (1, 'Alfred', false, [2,8,12,14]);");
		run("insert into lwt_example_db.players (player_id, name, assigned, stats) values (2, 'Wanda', false, [3,14,22,34]);");
		run("insert into lwt_example_db.players (player_id, name, assigned, stats) values (3, 'Wilma', false, [0,31,39,44]);");
		run("insert into lwt_example_db.players (player_id, name, assigned, stats) values (4, 'Alta', false, [2,58,51,64]);");
		run("insert into lwt_example_db.players (player_id, name, assigned, stats) values (5, 'Bajo', false, [5,19,29,39]);");
	}

	public void insertTeams() {
		run("insert into lwt_example_db.team (team_id, name) values (1, 'Flinstones');");
		run(" insert into lwt_example_db.team (team_id, name) values (2, 'Jetsons');");
	}


	public void insertPlayerIntoTeam(int playerId, int teamId) {
		boolean lwtSucess = runLWT("UPDATE lwt_example_db.players SET assigned = true WHERE player_id = 1 IF assigned = false;");
		if (lwtSucess) {
			run("INSERT into lwt_example_db.team (team_id, player_id, player_name) values (1, 1, 'Alfred');");
			run("INSERT into lwt_example_db.team_roster_log (log_id, event_type, to_team_id, player_id, player_name) values (1, 'ADD', 1, 1, 'Alfred');");
		}
		else {
			logger.info("Player is already on a team");
		}
	}

}

/**
 *
 *
 *

 insert into players (player_id, name, assigned, stats) values (1, 'Alfred', false, [2,8,12,14]);
 insert into players (player_id, name, assigned, stats) values (2, 'Wanda', false, [3,14,22,34]);
 insert into players (player_id, name, assigned, stats) values (3, 'Wilma', false, [0,31,39,44]);
 insert into players (player_id, name, assigned, stats) values (4, 'Alta', false, [2,58,51,64]);
 insert into players (player_id, name, assigned, stats) values (5, 'Bajo', false, [5,19,29,39]);

 insert into team (team_id, name) values (1, 'Flinstones');
 insert into team (team_id, name) values (2, 'Jetsons');

 BEGIN BATCH
 UPDATE players SET assigned = true WHERE player_id = 1 IF assigned = false;
 INSERT into team (team_id, player_id, player_name) values (1, 1, 'Alfred');
 APPLY BATCH;

 INSERT into team_roster_log (log_id, event_type, to_team_id, player_id, player_name) values (1, 'ADD', 1, 1, 'Alfred');


 * */