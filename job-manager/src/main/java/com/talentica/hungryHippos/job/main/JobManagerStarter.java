package com.talentica.hungryHippos.job.main;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.job.JobMatrix;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.ZkSignalListener;
import com.talentica.hungryHippos.job.context.JobManagerApplicationContext;
import com.talentica.hungryHippos.master.job.JobManager;

/**
 * @author PooshanS
 *
 */
public class JobManagerStarter {

	/**
	 * @param args
	 */
	private static final Logger LOGGER = LoggerFactory
			.getLogger(JobManagerStarter.class);
	private static NodesManager nodesManager;
	private static String jobUUId;

	public static void main(String[] args) {
		try {
			validateProgramArguments(args);
			initialize(args);
			long startTime = System.currentTimeMillis();
			JobManager jobManager = new JobManager();
			JobManager.nodesManager = nodesManager;
			jobManager.addJobList(((JobMatrix) getJobMatrix(args))
					.getListOfJobsToExecute());
			jobManager.start(args[1]);
			long endTime = System.currentTimeMillis();
			LOGGER.info("It took {} seconds of time to for running all jobs.",
					((endTime - startTime) / 1000));
		} catch (Exception exception) {
			errorHandler(exception);
		}
	}

	/**
	 * @param exception
	 */
	private static void errorHandler(Exception exception) {
		LOGGER.error("Error occured while executing master starter program.",
				exception);
		try {
			ZkSignalListener.createErrorEncounterSignal(nodesManager);
		} catch (IOException | InterruptedException e) {
			LOGGER.info("Unable to create the node on zk due to {}",
					e.getMessage());
		}
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	private static void initialize(String[] args) throws Exception {
		jobUUId = args[1];
		CommonUtil.loadDefaultPath(jobUUId);
		ZkSignalListener.jobuuidInBase64 = CommonUtil
				.getJobUUIdInBase64(jobUUId);
		JobManagerStarter.nodesManager = JobManagerApplicationContext.getNodesManager();
	}

	private static void validateProgramArguments(String[] args)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		if (args.length < 2) {
			System.out
					.println("Please provide the required jobn matrix class name argument to be able to run the program and second argument as jobuuid.");
			System.exit(1);
		}
		Object jobMatrix = getJobMatrix(args);
		if (!(jobMatrix instanceof JobMatrix)) {
			System.out
					.println("First argument should be a valid job matrix class name in the class-path.");
			System.exit(1);
		}
	}

	private static Object getJobMatrix(String[] args)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		Object jobMatrix = Class.forName(args[0]).newInstance();
		return jobMatrix;
	}

}
