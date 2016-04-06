package com.talentica.hungryHippos.job.main;

import java.io.FileNotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.job.JobMatrix;
import com.talentica.hungryHippos.coordination.utility.Property;
import com.talentica.hungryHippos.coordination.utility.Property.PROPERTIES_NAMESPACE;
import com.talentica.hungryHippos.master.job.JobManager;

/**
 * @author PooshanS
 *
 */
public class JobManagerStarter {

	/**
	 * @param args
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(JobManagerStarter.class);

	public static void main(String[] args) {
		try {
			long startTime = System.currentTimeMillis();
			validateProgramArguments(args);
			Property.initialize(PROPERTIES_NAMESPACE.NODE);
			overrideProperties(args);
			JobManager jobManager = new JobManager();
			jobManager.addJobList(((JobMatrix) getJobMatrix(args)).getListOfJobsToExecute());
			jobManager.start();
			long endTime = System.currentTimeMillis();
			LOGGER.info("It took {} seconds of time to for running all jobs.", ((endTime - startTime) / 1000));
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
			Property.overrideConfigurationProperties(args[1]);
		}
	}

}
