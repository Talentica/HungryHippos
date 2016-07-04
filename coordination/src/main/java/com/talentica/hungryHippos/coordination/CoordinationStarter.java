package com.talentica.hungryHippos.coordination;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.client.ClientConfig;
import com.talentica.hungryhippos.config.coordination.CoordinationConfig;

/**
 * Starts coordination server and updates configuration about cluster
 * environment.
 * 
 * @author nitink
 *
 */
public class CoordinationStarter {

	private static final Logger LOGGER = LoggerFactory.getLogger(CoordinationStarter.class);

	public static void main(String[] args) {
		try {
			LOGGER.info("Starting coordination server..");
			validateArguments(args);
			String coordinationConfigFilePath = args[1];
			CoordinationConfig configuration = JaxbUtil.unmarshalFromFile(coordinationConfigFilePath,
					CoordinationConfig.class);
			if (configuration.getClusterConfig() == null || configuration.getClusterConfig().getNode().isEmpty()) {
				throw new RuntimeException(
						"Invalid configuration file or cluster configuration missing." + coordinationConfigFilePath);
			}
			ClientConfig clientConfig = JaxbUtil.unmarshalFromFile(args[0], ClientConfig.class);
			CoordinationApplicationContext.updateClusterConfiguration(
					FileUtils.readFileToString(new File(coordinationConfigFilePath), "UTF-8"),
					clientConfig.getCoordinationServers());
			LOGGER.info("Coordination server started..");
		} catch (Exception exception) {
			LOGGER.error("Error occurred while starting coordination server.", exception);
			throw new RuntimeException(exception);
		}
	}

	private static void validateArguments(String[] args) {
		if (args == null || args.length < 2) {
			LOGGER.error("Missing client configuration and coordination configuration file arguments.");
			System.exit(1);
		}
	}

}