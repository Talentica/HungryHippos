/**
 * 
 */
package com.talentica.hungryHippos.coordination.test;

import java.io.IOException;
import java.util.Properties;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.domain.ServerHeartBeat;
import com.talentica.hungryHippos.coordination.utility.Property;

/**
 * @author PooshanS
 *
 */
public class StartZookeeperTest {
	private final static Logger LOGGER = LoggerFactory
			.getLogger(StartZookeeperTest.class);
	Properties startupProperties = Property.getProperties();

	@Before
	public void start() {
		QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
		try {
			quorumConfiguration.parseProperties(startupProperties);
			quorumConfiguration.getServers();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		ZooKeeperServerMain zooKeeperServer = new ZooKeeperServerMain();
		final ServerConfig configuration = new ServerConfig();
		configuration.readFrom(quorumConfiguration);

		new Thread() {
			public void run() {
				try {
					zooKeeperServer.runFromConfig(configuration);
				} catch (IOException e) {
					LOGGER.info("ZooKeeper Failed", e);
				}
			}
		}.start();

	}
	
	@Test
	public void startZk() throws Exception{
		NodesManager manager;
		(manager = ServerHeartBeat.init()).connectZookeeper("localhost:2181").startup();
		manager.startup();
	}
	
}
