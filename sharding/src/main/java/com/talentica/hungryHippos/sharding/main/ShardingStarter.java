package com.talentica.hungryHippos.sharding.main;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.sharding.Sharding;
import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.Property.PROPERTIES_NAMESPACE;
import com.talentica.hungryHippos.utility.marshaling.Reader;

public class ShardingStarter {

	/**
	 * @param args
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(ShardingStarter.class);

	public static void main(String[] args) {
		try {
			Property.setNamespace(PROPERTIES_NAMESPACE.MASTER);
			overrideProperties(args);
			LOGGER.info("SHARDING STARTED");
			Sharding.doSharding(getInputReaderForSharding());
			LOGGER.info("SHARDING DONE!!");
		} catch (Exception exception) {
			LOGGER.error("Error occured while executing master starter program.", exception);
		}
	}

	private static void overrideProperties(String[] args) throws FileNotFoundException {
		if (args.length == 1) {
			LOGGER.info(
					"You have not provided external config.properties file. Default config.properties file will be use internally");
		} else if (args.length == 2) {
			Property.CONFIG_FILE = new FileInputStream(new String(args[1]));
		}
	}

	private static Reader getInputReaderForSharding() throws IOException {
		final String inputFile = Property.getProperties().getProperty("input.file");
		com.talentica.hungryHippos.utility.marshaling.FileReader fileReader = new com.talentica.hungryHippos.utility.marshaling.FileReader(
				inputFile);
		fileReader.setNumFields(9);
		fileReader.setMaxsize(25);
		return fileReader;
	}

}
