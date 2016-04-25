package com.talentica.hungryHippos.sharding.main;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZKUtils;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.Property;
import com.talentica.hungryHippos.coordination.utility.Property.PROPERTIES_NAMESPACE;
import com.talentica.hungryHippos.coordination.utility.marshaling.Reader;
import com.talentica.hungryHippos.sharding.Sharding;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryHippos.utility.ZKNodeName;

public class ShardingStarter {

	/**
	 * @param args
	 */
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ShardingStarter.class);
	private static NodesManager nodesManager;
	private static final String sampleInputFile = "sample_input.txt";

	public static void main(String[] args) {
		try {
			long startTime = System.currentTimeMillis();
			Property.initialize(PROPERTIES_NAMESPACE.MASTER);
			ShardingStarter.nodesManager = Property.getNodesManagerIntances();
			callDownloadShellScript();
			LOGGER.info("WATING FOR THE SIGNAL OF DOWNLOAD COMPLETION.");
			waitForDownloadSinal();
			LOGGER.info("SIGNAL RECIEVED, DOWNLOAD IS COMPLETED.");
			callSamplingShellScript();
			LOGGER.info("WATING FOR THE SIGNAL OF SMAPLING COMPLETION.");
			waitForSamplingSinal();
			LOGGER.info("SIGNAL RECIEVED, SAMPLING IS COMPLETED.");
			LOGGER.info("SHARDING STARTED");
			Sharding.doSharding(getInputReaderForSharding(),
					ShardingStarter.nodesManager);
			LOGGER.info("SHARDING DONE!!");
			long endTime = System.currentTimeMillis();
			LOGGER.info("It took {} seconds of time to do sharding.",
					((endTime - startTime) / 1000));
		} catch (Exception exception) {
			LOGGER.error("Error occured while executing sharding program.",
					exception);
		}
	}

	/**
	 * 
	 */
	private static void callDownloadShellScript() {
		String webserverIp = Property.getProperties().getProperty(
				"common.webserver.ip");
		String jobuuid = Property.getProperties().getProperty("job.uuid");
		String downloadUrlLink = Property.getProperties().getProperty(
				"input.file.url.link");
		LOGGER.info(
				"Calling shell script to download the input file from url link {} for jobuuid {} and webserver ip {}",
				downloadUrlLink, jobuuid, webserverIp);
		String downloadScriptPath = Paths
				.get("/root/hungryhippos/scripts/bash_scripts")
				.toAbsolutePath().toString()
				+ PathUtil.FORWARD_SLASH;
		String[] strArr = new String[] { "/bin/sh",
				downloadScriptPath + "download-file-from-url.sh",
				downloadUrlLink, jobuuid, webserverIp };
		CommonUtil.executeScriptCommand(strArr);
		LOGGER.info("Downloading is initiated.");
	}

	private static void callSamplingShellScript() {
		String jobuuid = Property.getProperties().getProperty("job.uuid");
		String webserverIp = Property.getProperties().getProperty(
				"common.webserver.ip");
		LOGGER.info(
				"Calling sampling python script and uuid {} webserver ip {}",
				jobuuid, webserverIp);
		String samplingScriptPath = Paths
				.get("/root/hungryhippos/scripts/python_scripts")
				.toAbsolutePath().toString()
				+ PathUtil.FORWARD_SLASH;
		String[] strArr = new String[] { "/usr/bin/python",
				samplingScriptPath + "sampling-input-file.py", jobuuid,
				webserverIp };
		CommonUtil.executeScriptCommand(strArr);
		LOGGER.info("Sampling is initiated.");
	}

	private static Reader getInputReaderForSharding() throws IOException {
		// final String inputFile =
		// Property.getPropertyValue("input.file").toString();
		return new com.talentica.hungryHippos.coordination.utility.marshaling.FileReader(
				sampleInputFile);
	}

	private static void waitForSamplingSinal() throws Exception,
			KeeperException, InterruptedException {
		CountDownLatch signal = new CountDownLatch(1);
		ZKUtils.waitForSignal(ShardingStarter.nodesManager
				.buildAlertPathByName(ZKNodeName.SAMPLING_COMPLETED), signal);
		signal.await();
	}

	private static void waitForDownloadSinal() throws Exception,
			KeeperException, InterruptedException {
		CountDownLatch signal = new CountDownLatch(1);
		ZKUtils.waitForSignal(ShardingStarter.nodesManager
				.buildAlertPathByName(ZKNodeName.INPUT_DOWNLOAD_COMPLETED),
				signal);
		signal.await();
	}

}
