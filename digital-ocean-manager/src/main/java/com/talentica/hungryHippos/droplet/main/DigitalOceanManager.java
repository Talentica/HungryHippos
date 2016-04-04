package com.talentica.hungryHippos.droplet.main;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.myjeeva.digitalocean.exception.DigitalOceanException;
import com.myjeeva.digitalocean.exception.RequestUnsuccessfulException;
import com.talentica.hungryHippos.droplet.DigitalOceanServiceImpl;
import com.talentica.hungryHippos.droplet.entity.DigitalOceanEntity;
import com.talentica.hungryHippos.droplet.util.DigitalOceanServiceUtil;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.Property.PROPERTIES_NAMESPACE;

/**
 * @author PooshanS
 *
 */
public class DigitalOceanManager {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(DigitalOceanManager.class);
	private static DigitalOceanServiceImpl dropletService;
	private static ObjectMapper mapper = new ObjectMapper();
	private static String scriptPath;
	public static void main(String[] args) throws Exception {
		try {
			if (args.length == 2) {
				Property.overrideConfigurationProperties(args[1]);
			} else {
				LOGGER.info("Please provide the argument.First argument is json and second argument is config file.");
				return;
			}
			List<String> s = new ArrayList<String>();
			s.add("Hello");
			DigitalOceanServiceUtil.writeLineInFile(scriptPath, s);
			LOGGER.info(scriptPath);
			Property.initialize(PROPERTIES_NAMESPACE.ZK);
			validateProgramArguments(args);
			DigitalOceanEntity dropletEntity = getDropletEntity(args);
			dropletService = new DigitalOceanServiceImpl(
					dropletEntity.getAuthToken());
			DigitalOceanServiceUtil.performServices(dropletService,
					dropletEntity);
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException | RequestUnsuccessfulException
				| DigitalOceanException | IOException | InterruptedException e) {
			LOGGER.info("Unable to perform the operations {}", e);
		}
	}

	/**
	 * @param args
	 * @return
	 * @throws IOException
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 */
	private static DigitalOceanEntity getDropletEntity(String[] args)
			throws IOException, JsonParseException, JsonMappingException {
		String filePath = args[0];
		File file = new File(filePath);
		DigitalOceanEntity dropletEntity = mapper.readValue(file,
				DigitalOceanEntity.class);
		return dropletEntity;
	}

	private static void validateProgramArguments(String[] args)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		if (args.length < 1) {
			System.out.println("Please provide the json file as argument");
			System.exit(1);
		}
	}

}
