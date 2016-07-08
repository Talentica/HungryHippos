package com.talentica.hungryHippos.master.job;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.utility.JobEntity;

public class NodeJobsServiceTest {

	private NodeJobsService nodeService = null;;
	private Node node = null;
	private NodesManager nodesManager = null;
	private long nodeCapacity = 2000l;
	private int nodeId = 1;
	private String zookeeperConfigPath = "/home/pooshans/HungryHippos/configuration-schema/src/main/resources/schema/zookeeper-config.xml";
	@Before
	public void setUp() throws Exception {
		node = new Node(nodeCapacity, nodeId);
		nodesManager = new NodesManager(zookeeperConfigPath);
		nodeService = new NodeJobsService(node, nodesManager);
	}

	@After
	public void tearDown() throws Exception {
		node = null;
		nodesManager = null;
		nodeService = null;
	}

	@Test
	public void testNodeJobsService() {
		assertNotNull(nodeService);
	}

	@Test
	public void testCreateNodeJobService() {
		JobEntity jobEntity = new JobEntity();
		nodeService.addJob(jobEntity);
		try {
			nodeService.createNodeJobService();
		} catch (ClassNotFoundException | IOException | InterruptedException | KeeperException e) {
			assertTrue(false);
		}
		assertTrue(jobEntity.getStatus().equals("POOLED"));
		assertNotNull(nodeService.getTaskManager());
	}

	@Test
	public void testSendJobRunnableNotificationToNode() {
		JobEntity jobEntity = new JobEntity();
		CountDownLatch signal = new CountDownLatch(1);
		String jobUUId = "123";
		nodesManager.connectZookeeper("localhost:2181");
		try {
			nodeService.sendJobRunnableNotificationToNode(jobEntity, signal, jobUUId);
		} catch (ClassNotFoundException | IOException | InterruptedException | KeeperException e) {
			assertTrue(false);
		}
	}

	@Ignore
	@Test
	public void testReceiveJobSucceedNotificationFromNode() {
		fail("Not yet implemented");
	}
	

}
