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

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;

import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.utility.JobEntity;
import com.talentica.hungryhippos.config.coordination.CoordinationConfig;

public class NodeJobsServiceIntegrationTest {

  private NodeJobsService nodeService = null;;
  private Node node = null;
  private HungryHippoCurator curator = null;
  private long nodeCapacity = 2000l;
  private int nodeId = 1;
  private String clientConfigFilePath =
      "/home/sohanc/D_drive/HungryHippos_project/HungryHippos/configuration-schema/src/main/resources/schema/client-config.xml";

  @Before
  public void setUp() throws Exception {
    node = new Node(nodeCapacity, nodeId);
    curator = HungryHippoCurator.getInstance("localhost:2181");
    CoordinationConfig coordinationConfig = CoordinationConfigUtil.getZkCoordinationConfigCache();
    curator.initializeZookeeperDefaultConfig(coordinationConfig.getZookeeperDefaultConfig());
    nodeService = new NodeJobsService(node, curator);
  }

  @After
  public void tearDown() throws Exception {
    node = null;
    curator = null;
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
