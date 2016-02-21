package com.datastax.sample.webservice;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Set;

import javax.jws.WebService;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.sample.service.Service;

@WebService
@Path("/")
public class SampleWS {

	private Logger logger = LoggerFactory.getLogger(SampleWS.class);
	private SimpleDateFormat inputDateFormat = new SimpleDateFormat("yyyyMMdd");

	//Service Layer.
	private Service service = new Service();
	
	@GET
	@Path("/get/keyspaces")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getMovements() {
				
		List<KeyspaceMetadata> keyspaces = service.getKeyspaces();		
		return Response.status(Status.OK).entity(keyspaces.toString()).build();
	}
}
