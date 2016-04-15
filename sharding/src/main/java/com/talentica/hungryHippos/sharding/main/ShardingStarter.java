package com.talentica.hungryHippos.sharding.main;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.Property;
import com.talentica.hungryHippos.coordination.utility.Property.PROPERTIES_NAMESPACE;
import com.talentica.hungryHippos.coordination.utility.marshaling.Reader;
import com.talentica.hungryHippos.sharding.Sharding;

public class ShardingStarter {

	/**
	 * @param args
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(ShardingStarter.class);

	public static void main(String[] args) {
		try {
			long startTime = System.currentTimeMillis();
			Property.initialize(PROPERTIES_NAMESPACE.MASTER);
			CommonUtil.connectZK();
			LOGGER.info("SHARDING STARTED");
			Sharding.doSharding(getInputReaderForSharding());
			LOGGER.info("SHARDING DONE!!");
			long endTime = System.currentTimeMillis();
			LOGGER.info("It took {} seconds of time to do sharding.", ((endTime - startTime) / 1000));
		} catch (Exception exception) {
			LOGGER.error("Error occured while executing sharding program.", exception);
		}
	}

	private static void overrideProperties(String[] args) throws FileNotFoundException {
		if (args.length == 1) {
			LOGGER.info(
					"You have not provided external config.properties file. Default config.properties file will be use internally");
		} else if (args.length == 2) {
			Property.overrideConfigurationProperties(args[1]);
		}
	}

	private static Reader getInputReaderForSharding() throws IOException {
		final String inputFile = Property.getPropertyValue("input.file").toString();
		return new com.talentica.hungryHippos.coordination.utility.marshaling.FileReader(
				inputFile);
	}

}
