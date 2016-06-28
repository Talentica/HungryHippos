package com.talentica.hungryHippos.coordination;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;

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
			validateArguments(args);
			CoordinationApplicationContext.updateClusterSetup();
		} catch (IOException exception) {
			LOGGER.error("Error occurred while starting coordination server.", exception);
			throw new RuntimeException(exception);
		}
	}

	private static void validateArguments(String[] args) {
		if (args == null || args.length < 1) {
			LOGGER.error("Missing coordination configuration file path.");
			System.exit(1);
		}
	}

}