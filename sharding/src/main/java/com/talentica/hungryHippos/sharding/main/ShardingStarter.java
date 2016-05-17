package com.talentica.hungryHippos.sharding.main;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZKUtils;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.ENVIRONMENT;
import com.talentica.hungryHippos.coordination.utility.Property;
import com.talentica.hungryHippos.coordination.utility.Property.PROPERTIES_NAMESPACE;
import com.talentica.hungryHippos.coordination.utility.ScriptExecutionUtil;
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
			long startTime = System.currentTimeMillis();
			String jobUUId = args[0];
			CommonUtil.loadDefaultPath(jobUUId);
			Property.initialize(PROPERTIES_NAMESPACE.MASTER);
			ZKUtils.createDefaultNodes(jobUUId);
			ShardingStarter.nodesManager = Property.getNodesManagerIntances();
			callShellScriptBeforeSharding();
			LOGGER.info("SHARDING STARTED");
			Sharding.doSharding(getInputReaderForSharding());
			LOGGER.info("SHARDING DONE!!");
			long endTime = System.currentTimeMillis();
			LOGGER.info("It took {} seconds of time to do sharding.",
					((endTime - startTime) / 1000));
			callShellScriptAfterSharding();
		} catch (Exception exception) {
			LOGGER.error("Error occurred while sharding.", exception);
			sendShardingFailSignal();
		}
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
	private static void callShellScriptAfterSharding() throws IOException,
			InterruptedException {
		ScriptExecutionUtil.callCopyScriptForMapFiles();
		ZkSignalListener.sendSignal(nodesManager,
				CommonUtil.ZKJobNodeEnum.SHARDING_COMPLETED.getZKJobNode());
	}

	/**
	 * @throws Exception
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	private static void callShellScriptBeforeSharding() throws Exception,
			KeeperException, InterruptedException {
		ScriptExecutionUtil.callDownloadShellScript();
		LOGGER.info("WATING FOR THE SIGNAL OF DOWNLOAD COMPLETION.");
		if (ENVIRONMENT.getCurrentEnvironment() != ENVIRONMENT.LOCAL)
			ZkSignalListener.waitForDownloadSinal(ShardingStarter.nodesManager,
					CommonUtil.ZKJobNodeEnum.INPUT_DOWNLOAD_COMPLETED
							.getZKJobNode());
		LOGGER.info("SIGNAL RECIEVED, DOWNLOAD IS COMPLETED.");
		ScriptExecutionUtil.callSamplingShellScript();
		LOGGER.info("WATING FOR THE SIGNAL OF SMAPLING COMPLETION.");
		if (ENVIRONMENT.getCurrentEnvironment() != ENVIRONMENT.LOCAL)
			ZkSignalListener.waitForSamplingSinal(ShardingStarter.nodesManager,
					CommonUtil.ZKJobNodeEnum.SAMPLING_COMPLETED.getZKJobNode());
		LOGGER.info("SIGNAL RECIEVED, SAMPLING IS COMPLETED.");
	}

	private static Reader getInputReaderForSharding() throws IOException {
		return new com.talentica.hungryHippos.coordination.utility.marshaling.FileReader(
				sampleInputFile);
	}

}
