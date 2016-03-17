/**
 * 
 */
package com.talentica.hungryHippos.droplet.main;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.myjeeva.digitalocean.exception.DigitalOceanException;
import com.myjeeva.digitalocean.exception.RequestUnsuccessfulException;
import com.myjeeva.digitalocean.pojo.Droplet;
import com.talentica.hungryHippos.droplet.DigitalOceanDropletService;

/**
 * @author PooshanS
 *
 */
public class DigitalOceanManager {

	private static final Logger LOGGER = LoggerFactory.getLogger(DigitalOceanManager.class);
	private static String authToken = "02eba1965ba9ba368f57b79f454e05244a7fc7e2b2f7d60d9c235f6009fe5ad9";
	private static DigitalOceanDropletService dropletService;
	private static ObjectMapper mapper;
	public static void main(String[] args) {
			try {
				validateProgramArguments(args);
				String filePath = args[0];
				dropletService = new DigitalOceanDropletService(authToken);
				mapper = new ObjectMapper();
				File file = new File(filePath);
				Droplet newDroplet = mapper.readValue(file, Droplet.class);
				newDroplet = dropletService.createDroplet(newDroplet);
			} catch (InstantiationException | IllegalAccessException
					| ClassNotFoundException | RequestUnsuccessfulException | DigitalOceanException | IOException e) {
				LOGGER.info("Unable to create the droplet/droples");
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
