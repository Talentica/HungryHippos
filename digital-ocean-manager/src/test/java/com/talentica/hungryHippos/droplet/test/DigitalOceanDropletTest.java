/**
 * 
 */
package com.talentica.hungryHippos.droplet.test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.myjeeva.digitalocean.exception.DigitalOceanException;
import com.myjeeva.digitalocean.exception.RequestUnsuccessfulException;
import com.myjeeva.digitalocean.pojo.Delete;
import com.myjeeva.digitalocean.pojo.Droplet;
import com.myjeeva.digitalocean.pojo.Droplets;
import com.talentica.hungryHippos.droplet.DigitalOceanDropletService;

/**
 * @author PooshanS
 *
 */
public class DigitalOceanDropletTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(DigitalOceanDropletTest.class);
	private DigitalOceanDropletService dropletService;
	private int dropletId = 12003540;
	
	private ObjectMapper mapper;
	
	private String authToken = "02eba1965ba9ba368f57b79f454e05244a7fc7e2b2f7d60d9c235f6009fe5ad9";
	private Integer[] dropletIds = new Integer[]{11972764,11972765,11972766,11972767,11972768,11972769,11972770,11972771,11972772,11972773,12026346};
	
	@Before
	public void setUp(){
		dropletService = new DigitalOceanDropletService(authToken);
		mapper = new ObjectMapper();
	}
	
	@Test
	@Ignore
	public void testCaseCreateDroplet() throws DigitalOceanException, RequestUnsuccessfulException, JsonParseException, JsonMappingException, IOException{
		File file = new File(".\\droplet.json");
		Droplet newDroplet = mapper.readValue(file, Droplet.class);
		try {
			newDroplet = dropletService.createDroplet(newDroplet);
			Assert.assertNotNull(newDroplet.getId());
		} catch (RequestUnsuccessfulException | DigitalOceanException e) {
			System.out.println("Unable to create the droplet. Exception :: "+e);
		}
	}
	
	@Test
	public void testCaseListAllDroplets(){
		try {
			Droplets droplets = dropletService.getAvailableDroplets(1, 10);
			Assert.assertNotNull(droplets);
		} catch (DigitalOceanException | RequestUnsuccessfulException e) {
			LOGGER.info("Unable to fetch the records of all droplets {}",e.getMessage());
		}
	}
	
	@Test
	@Ignore
	public void testCaseFindDroplet(){
		try {
			Droplet droplet = dropletService.getDropletInfo(dropletId);
			Assert.assertNotNull(droplet);
		} catch (DigitalOceanException | RequestUnsuccessfulException e) {
			LOGGER.info("Unable to fetch the record of droplet of id {} and exception is {}",new Object[]{dropletId,e.getMessage()});
		}
	}
	
	@Test
	@Ignore
	public void testCaseDeleteDroplet(){
		try {
			Delete delete = dropletService.deleteDroplet(dropletId);
			Assert.assertEquals(204,delete.getStatusCode());
		} catch (RequestUnsuccessfulException | DigitalOceanException e) {
			LOGGER.info("Unable to drop the droplet of id {} and exception is {}",new Object[]{dropletId,e.getMessage()});
		}
	}
	
	@Test
	@Ignore
	public void testCaseDeleteAllDroplet(){
		List<Delete> deletes = null;
		try {
			deletes = dropletService.deleteDroplets(Arrays.asList(dropletIds));
			LOGGER.info("All droplets are deleted {}",deletes.toString());
		} catch (RequestUnsuccessfulException | DigitalOceanException e) {
			LOGGER.info("Unable to drop the droplet of id {} and exception is {}",new Object[]{dropletIds,e.getMessage()});
		}
	}
	
}
