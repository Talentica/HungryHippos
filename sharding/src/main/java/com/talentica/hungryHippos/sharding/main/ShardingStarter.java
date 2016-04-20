package com.talentica.hungryHippos.sharding.main;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZKUtils;
import com.talentica.hungryHippos.coordination.utility.Property;
import com.talentica.hungryHippos.coordination.utility.Property.PROPERTIES_NAMESPACE;
import com.talentica.hungryHippos.coordination.utility.marshaling.Reader;
import com.talentica.hungryHippos.sharding.Sharding;
import com.talentica.hungryHippos.utility.ZKNodeName;

public class ShardingStarter {

	/**
	 * @param args
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(ShardingStarter.class);
	private static NodesManager nodesManager;
	private static final String sampleInputFile = "sample_input.txt";

	public static void main(String[] args) {
		try {
			long startTime = System.currentTimeMillis();
			Property.initialize(PROPERTIES_NAMESPACE.MASTER);
			ShardingStarter.nodesManager = Property.getNodesManagerIntances();
			LOGGER.info("WATING FOR THE SIGNAL OF SMAPLING COMPLETION.");
			waitForSinal();
			LOGGER.info("SIGNAL RECIEVED, SAMPLING IS COMPLETED.");
			LOGGER.info("SHARDING STARTED");
			Sharding.doSharding(getInputReaderForSharding(),ShardingStarter.nodesManager);
			LOGGER.info("SHARDING DONE!!");
			long endTime = System.currentTimeMillis();
			LOGGER.info("It took {} seconds of time to do sharding.", ((endTime - startTime) / 1000));
		} catch (Exception exception) {
			LOGGER.error("Error occured while executing sharding program.", exception);
		}
	}

	private static Reader getInputReaderForSharding() throws IOException {
		//final String inputFile = Property.getPropertyValue("input.file").toString();
		return new com.talentica.hungryHippos.coordination.utility.marshaling.FileReader(sampleInputFile);
	}
	
	private static void waitForSinal()
			throws Exception, KeeperException, InterruptedException {
		CountDownLatch signal = new CountDownLatch(1);
		ZKUtils.waitForSignal(ShardingStarter.nodesManager.buildAlertPathByName(ZKNodeName.SHAMPLING_COMPLETED), signal);
		signal.await();
	}

}
