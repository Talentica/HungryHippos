package com.talentica.hungryHippos.job.main;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.job.JobMatrix;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.Property;
import com.talentica.hungryHippos.coordination.utility.Property.PROPERTIES_NAMESPACE;
import com.talentica.hungryHippos.coordination.utility.ZkSignalListener;
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

	public static void main(String[] args) {
		try {
			validateProgramArguments(args);
			initialize(args);
			listenerOnDataPublishCompletion();
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
	 */
	private static void initialize(String[] args) {
		String jobUUId = args[1];
		ZkSignalListener.jobuuidInBase64 = CommonUtil
				.getJobUUIdInBase64(jobUUId);
		CommonUtil.loadDefaultPath(jobUUId);
		Property.initialize(PROPERTIES_NAMESPACE.NODE);
		JobManagerStarter.nodesManager = Property.getNodesManagerIntances();
	}

	/**
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	private static void listenerOnDataPublishCompletion()
			throws KeeperException, InterruptedException {
		ZkSignalListener.waitForCompletion(JobManager.nodesManager,
				CommonUtil.ZKJobNodeEnum.DATA_PUBLISHING_COMPLETED
						.getZKJobNode());
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
