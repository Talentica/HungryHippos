/**
 * 
 */
package com.talentica.hungryHippos.droplet.main;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.myjeeva.digitalocean.exception.DigitalOceanException;
import com.myjeeva.digitalocean.exception.RequestUnsuccessfulException;
import com.myjeeva.digitalocean.pojo.Delete;
import com.myjeeva.digitalocean.pojo.Droplet;
import com.talentica.hungryHippos.droplet.DigitalOceanDropletService;
import com.talentica.hungryHippos.droplet.entity.DigitalOceanEntity;

/**
 * @author PooshanS
 *
 */
public class DigitalOceanManager {

	private static final Logger LOGGER = LoggerFactory.getLogger(DigitalOceanManager.class);
	private static DigitalOceanDropletService dropletService;
	private static ObjectMapper mapper = new ObjectMapper();
	public static void main(String[] args) {
		try {
			validateProgramArguments(args);
			String filePath = args[0];
			File file = new File(filePath);
			DigitalOceanEntity dropletEntity = mapper.readValue(file,
					DigitalOceanEntity.class);
			dropletService = new DigitalOceanDropletService(
					dropletEntity.getAuthToken());
			switch (dropletEntity.getRequest()) {
			case "POST":
				Droplet droplet = dropletService.createDroplet(dropletEntity
						.getDroplet());
				LOGGER.info("Droplet of id {} is created", droplet.getId());
				break;
			case  "DELETE":
				List<Delete> deleteList =  dropletService.deleteDroplets(dropletEntity.getIdsAsList());
				LOGGER.info("There list are deleted {}",deleteList.toString());
			default:	
				break;

			}
		} catch (InstantiationException | IllegalAccessException
					| ClassNotFoundException | RequestUnsuccessfulException | DigitalOceanException | IOException e) {
				LOGGER.info("Unable to create the droplet/droples");
				e.getStackTrace();
			}
	}
	
	private static void validateProgramArguments(String[] args)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		if (args.length < 1) {
			System.out.println("Please provide the json file as argument");
			System.exit(1);
		}
	}

}
