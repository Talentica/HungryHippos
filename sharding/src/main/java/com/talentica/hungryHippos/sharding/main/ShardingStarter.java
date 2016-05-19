package com.talentica.hungryHippos.sharding.main;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZKUtils;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.ENVIRONMENT;
import com.talentica.hungryHippos.coordination.utility.Property;
import com.talentica.hungryHippos.coordination.utility.Property.PROPERTIES_NAMESPACE;
import com.talentica.hungryHippos.coordination.utility.ZkSignalListener;
import com.talentica.hungryHippos.coordination.utility.marshaling.Reader;
import com.talentica.hungryHippos.sharding.Sharding;

public class ShardingStarter {

	/**
	 * @param args
	 */
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ShardingStarter.class);
	private static NodesManager nodesManager;
	private static final String sampleInputFile = "sisample1mw.txt";

	public static void main(String[] args) {
		try {
			initialize(args);
			LOGGER.info("SHARDING STARTED");
			long startTime = System.currentTimeMillis();
			Sharding.doSharding(getInputReaderForSharding());
			LOGGER.info("SHARDING DONE!!");
			long endTime = System.currentTimeMillis();
			LOGGER.info("It took {} seconds of time to do sharding.",
					((endTime - startTime) / 1000));
			sendSignalShardingCompleted();
		} catch (Exception exception) {
			LOGGER.error("Error occurred while sharding.", exception);
			sendShardingFailSignal();
		}
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	private static void initialize(String[] args) throws Exception {
		String jobUUId = args[0];
		ZkSignalListener.jobuuidInBase64 = CommonUtil
				.getJobUUIdInBase64(jobUUId);
		CommonUtil.loadDefaultPath(jobUUId);
		Property.initialize(PROPERTIES_NAMESPACE.MASTER);
		if (ENVIRONMENT.getCurrentEnvironment() == ENVIRONMENT.LOCAL)
			ZKUtils.createDefaultNodes(jobUUId);
		ShardingStarter.nodesManager = Property.getNodesManagerIntances();
	}

	/**
	 * 
	 */
	private static void sendShardingFailSignal() {
		try {
			ZkSignalListener.shardingFailed(nodesManager,
					CommonUtil.ZKJobNodeEnum.SHARDING_FAILED.getZKJobNode());
		} catch (IOException | InterruptedException e) {
			LOGGER.info("Unable to create the node on zk.");
		}
	}

	/**
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private static void sendSignalShardingCompleted() throws IOException,
			InterruptedException {
		ZkSignalListener.sendSignal(nodesManager,
				CommonUtil.ZKJobNodeEnum.SHARDING_COMPLETED.getZKJobNode());
	}

	private static Reader getInputReaderForSharding() throws IOException {
		return new com.talentica.hungryHippos.coordination.utility.marshaling.FileReader(
				sampleInputFile);
	}

}
