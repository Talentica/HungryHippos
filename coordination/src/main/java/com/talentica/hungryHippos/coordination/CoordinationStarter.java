package com.talentica.hungryHippos.coordination;

import java.io.FileNotFoundException;

import javax.xml.bind.JAXBException;

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
			CoordinationConfig configuration = readConfiguration(args[0]);
			CoordinationApplicationContext.updateClusterConfiguration(configuration.getClusterConfig());
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