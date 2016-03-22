package com.talentica.hungryHippos.droplet.main;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.myjeeva.digitalocean.exception.DigitalOceanException;
import com.myjeeva.digitalocean.exception.RequestUnsuccessfulException;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.domain.ServerHeartBeat;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
import com.talentica.hungryHippos.droplet.DigitalOceanServiceImpl;
import com.talentica.hungryHippos.droplet.entity.DigitalOceanEntity;
import com.talentica.hungryHippos.droplet.util.DigitalOceanServiceUtil;
import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.Property.PROPERTIES_NAMESPACE;

/**
 * @author PooshanS
 *
 */
public class DigitalOceanManager {

	private static final Logger LOGGER = LoggerFactory.getLogger(DigitalOceanManager.class);
	private static DigitalOceanServiceImpl dropletService;
	private static ObjectMapper mapper = new ObjectMapper();
	private NodesManager nodesManager;

	public static void main(String[] args) throws Exception {
		try {
			if (args.length == 2) {
				Property.overrideConfigurationProperties(args[1]);
			}
			Property.initialize(PROPERTIES_NAMESPACE.MASTER);
			
			validateProgramArguments(args);
			DigitalOceanEntity dropletEntity = getDropletEntity(args);
			
			dropletService = new DigitalOceanServiceImpl(
					dropletEntity.getAuthToken());
			
			DigitalOceanManager digitalOceanManager = new DigitalOceanManager();
			
			DigitalOceanServiceUtil.performServices(dropletService,dropletEntity);
			
			startZookeeper(digitalOceanManager);
			
			uploadServerConfigFileToZK(digitalOceanManager);
			
			uploadConfigFileToZk(digitalOceanManager);
			
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException | RequestUnsuccessfulException
				| DigitalOceanException | IOException | InterruptedException e) {
			LOGGER.info("Unable to perform the operations {}",e.getMessage());
		}
	}

	/**
	 * @param digitalOceanManager
	 * @throws IOException
	 */
	private static void uploadConfigFileToZk(
			DigitalOceanManager digitalOceanManager) throws IOException {
		ZKNodeFile configNodeFile = new ZKNodeFile(Property.CONF_PROP_FILE + "_FILE", Property.getProperties());
		digitalOceanManager.nodesManager.saveConfigFileToZNode(configNodeFile, null);
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

	/**
	 * @param digitalOceanManager
	 * @throws IOException
	 */
	private static void uploadServerConfigFileToZK(
			DigitalOceanManager digitalOceanManager) throws IOException {
		LOGGER.info("PUT THE CONFIG FILE TO ZK NODE");
		ZKNodeFile serverConfigFile = new ZKNodeFile(Property.SERVER_CONF_FILE, Property.loadServerProperties());
		digitalOceanManager.nodesManager.saveConfigFileToZNode(serverConfigFile, null);
		LOGGER.info("serverConfigFile file successfully put on zk node.");
	}

	/**
	 * @param digitalOceanManager
	 * @throws Exception
	 */
	private static void startZookeeper(DigitalOceanManager digitalOceanManager)
			throws Exception {
		(digitalOceanManager.nodesManager = ServerHeartBeat.init()).startup();
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
