/**
 * 
 */
package com.talentica.hungryHippos.manager.main;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.job.JobMatrix;
import com.talentica.hungryHippos.manager.job.JobManager;
import com.talentica.hungryHippos.sharding.Sharding;
import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.Property.PROPERTIES_NAMESPACE;
import com.talentica.hungryHippos.utility.marshaling.Reader;

/**
 * @author PooshanS
 *
 */
public class MasterStarter {

	/**
	 * @param args
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(MasterStarter.class);

	public static void main(String[] args) {
		try {
			validateProgramArguments(args);
			Property.setNamespace(PROPERTIES_NAMESPACE.MASTER);
			overrideProperties(args);
			LOGGER.info("SHARDING STARTED");
			Sharding.doSharding(getInputReaderForSharding());
			LOGGER.info("SHARDING DONE!!");
			JobManager jobManager = new JobManager();
			jobManager.addJobList(((JobMatrix) getJobMatrix(args)).getListOfJobsToExecute());
			jobManager.start();
		} catch (Exception exception) {
			LOGGER.error("Error occured while executing master starter program.", exception);
		}
	}

	private static void validateProgramArguments(String[] args)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		if (args.length < 1) {
			System.out.println(
					"Please provide the required jobn matrix class name argument to be able to run the program.");
			System.exit(1);
		}
		Object jobMatrix = getJobMatrix(args);
		if (!(jobMatrix instanceof JobMatrix)) {
			System.out.println("First argument should be a valid job matrix class name in the class-path.");
			System.exit(1);
		}
	}

	private static Object getJobMatrix(String[] args)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		Object jobMatrix = Class.forName(args[0]).newInstance();
		return jobMatrix;
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
