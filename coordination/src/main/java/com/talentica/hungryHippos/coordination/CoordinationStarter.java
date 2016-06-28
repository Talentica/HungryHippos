package com.talentica.hungryHippos.coordination;

import java.io.File;
import java.io.FileNotFoundException;

import javax.xml.bind.JAXBException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
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
			String filePath = args[0];
			CoordinationConfig configuration = readConfiguration(filePath);
			if (configuration.getClusterConfig() == null || configuration.getClusterConfig().getNode().isEmpty()) {
				throw new RuntimeException("Invalid configuration file or cluster configuration missing." + filePath);
			}
			CoordinationApplicationContext
					.updateClusterConfiguration(FileUtils.readFileToString(new File(filePath), "UTF-8"));
			LOGGER.info("Coordination server started..");
		} catch (Exception exception) {
			LOGGER.error("Error occurred while starting coordination server.", exception);
			throw new RuntimeException(exception);
		}
	}

	private static CoordinationConfig readConfiguration(String filePath) throws FileNotFoundException, JAXBException {
		return JaxbUtil.unmarshalFromFile(filePath, CoordinationConfig.class);
	}

	private static void validateArguments(String[] args) {
		if (args == null || args.length < 1) {
			LOGGER.error("Missing coordination configuration file path.");
			System.exit(1);
		}
	}

}