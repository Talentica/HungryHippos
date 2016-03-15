/**
 * 
 */
package com.talentica.hungryHippos.utility;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.myjeeva.digitalocean.exception.DigitalOceanException;
import com.myjeeva.digitalocean.exception.RequestUnsuccessfulException;
import com.myjeeva.digitalocean.pojo.Delete;
import com.myjeeva.digitalocean.pojo.Droplet;
import com.myjeeva.digitalocean.pojo.Droplets;
import com.myjeeva.digitalocean.pojo.Image;
import com.myjeeva.digitalocean.pojo.Region;

/**
 * @author PooshanS
 *
 */
public class DigitalOceanDropletTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(DigitalOceanDropletTest.class);
	private DigitalOceanDropletService dropletService;
	private int dropletId = 12003540;
	
	private String authToken = "02eba1965ba9ba368f57b79f454e05244a7fc7e2b2f7d60d9c235f6009fe5ad9";
	
	@Before
	public void setUp(){
		dropletService = new DigitalOceanDropletService(authToken);
	}
	
	@Test
	@Ignore
	public void testCaseCreateDroplet() throws DigitalOceanException, RequestUnsuccessfulException{
		Droplet newDroplet = new Droplet();
		newDroplet.setName("api-client-test-host");
		newDroplet.setSize("512mb"); 
		newDroplet.setRegion(new Region("nyc3"));
		newDroplet.setImage(new Image("ubuntu-14-04-x64"));
		newDroplet.setEnableBackup(Boolean.TRUE);
		newDroplet.setEnableIpv6(Boolean.TRUE);
		newDroplet.setEnablePrivateNetworking(Boolean.TRUE);
		newDroplet.setKeys(dropletService.getAvailableKeys(1).getKeys());
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
	public void testCaseFindDroplet(){
		try {
			Droplet droplet = dropletService.getDropletInfo(dropletId);
			Assert.assertNotNull(droplet);
		} catch (DigitalOceanException | RequestUnsuccessfulException e) {
			LOGGER.info("Unable to fetch the record of droplet of id {} and exception is {}",new Object[]{dropletId,e.getMessage()});
		}
	}
	
	@Test
	public void testCaseDeleteDroplet(){
		try {
			Delete delete = dropletService.deleteDroplet(dropletId);
			Assert.assertEquals(204,delete.getStatusCode());
		} catch (RequestUnsuccessfulException | DigitalOceanException e) {
			LOGGER.info("Unable to drop the droplet of id {} and exception is {}",new Object[]{dropletId,e.getMessage()});
		}
	}
	
}
