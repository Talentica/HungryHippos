/**
 * 
 */
package com.talentica.hungryHippos.droplet.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author PooshanS
 *
 */
public class DigitalOceanManager {

	private static final Logger LOGGER = LoggerFactory.getLogger(DigitalOceanManager.class);
	
	public static void main(String[] args) {
		try {
			validateProgramArguments(args);
			
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException e) {
			LOGGER.info("Unable to create the droplet/droplets.");
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
