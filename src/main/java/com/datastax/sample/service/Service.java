package com.datastax.sample.service;

import java.util.List;
import java.util.Set;

import com.codahale.metrics.Gauge;
import com.datastax.demo.utils.PropertyHelper;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.sample.dao.SampleDao;

public class Service {

	private SampleDao dao;

	public Service() {		
		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		this.dao = new SampleDao(contactPointsStr.split(","));
	}	
	
	public List<KeyspaceMetadata> getKeyspaces() {
		return dao.getKeyspaces();
	}

	public void insertPlayers() {
		dao.insertPlayers();
	}

	public void insertTeams() {
		dao.insertTeams();
	}

	public void insertPlayerIntoTeam() {
		dao.insertPlayerIntoTeam(0,0);
	}
}
