/**
 * 
 */
package com.talentica.hungryHippos.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.domain.ServerHeartBeat;
import com.talentica.hungryHippos.master.data.DataProvider;
import com.talentica.hungryHippos.utility.CommonUtil.ZKNodeDeleteSignal;

public class DataPublisherStarter {

	private static NodesManager nodesManager;

	/**
	 * @param args
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(DataPublisherStarter.class);

	public static void main(String[] args) {
		try {

			LOGGER.info("Initializing nodes manager.");
			(nodesManager = ServerHeartBeat.init()).startup(ZKNodeDeleteSignal.MASTER.name());
			DataProvider.publishDataToNodes(nodesManager);
		} catch (Exception exception) {
			LOGGER.error("Error occured while executing publishing data on nodes.", exception);
		}
	}
}
