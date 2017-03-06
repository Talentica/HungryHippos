package com.talentica.hungryHippos.coordination;

import static org.junit.Assert.*;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.talentica.hungryHippos.coordination.exception.HungryHippoException;

@Ignore
public class HungryHippoCuratorTest {


  private String zookeeperConnectionString = "localhost:2181";
  private int baseSleepTime = 1000;
  private int maxRetry = 3;

  private HungryHippoCurator hhc = null;

  @Before
  public void setUp() throws Exception {
    hhc = HungryHippoCurator.getInstance(zookeeperConnectionString);

  }

  @After
  public void tearDown() throws Exception {
    hhc = null;
  }


  @Test
  public void testCreateZnode_2Arg() {
    String path = "/test";
    String data = "hi";
    String loc = null;
    /*
     * try { //loc = hhc.createPersistentNode(path, data); } catch (HungryHippoException e) {
     * assertTrue(false);// exception should not occur; }
     */
    assertEquals(path, loc);
  }

  @Test
  public void testCreateZnode() {
    String path = "/test1";
    String loc = null;
    try {
      loc = hhc.createPersistentNode(path);
    } catch (HungryHippoException e) {
      assertTrue(false);// exception should not occur;
    }
    assertEquals(path, loc);
  }

  @Test
  public void testCreateZnode_Seq_2Arg() {
    String path = "/test_seq";
    String data = "hi";
    String loc = null;
    try {
      loc = hhc.createPersistentZnodeSeq(path, data);
    } catch (HungryHippoException e) {
      assertTrue(false);// exception should not occur;
    }
    assertNotEquals(path, loc);
  }

  @Test
  public void testCreateZnode_Seq() {
    String path = "/test_seq2";
    String loc = null;
    try {
      loc = hhc.createPersistentZnodeSeq(path);
    } catch (HungryHippoException e) {
      assertTrue(false);// exception should not occur;
    }
    assertNotEquals(path, loc);
  }

  @Test
  public void testGetZnodeData() {
    String path = "/test";
    String data = null;
    try {
      data = hhc.getZnodeData(path);
    } catch (HungryHippoException e) {
      assertTrue(false);// exception should not occur;
    }
    assertEquals("hi", data);
  }

  @Test
  public void testCreateEphemeralNode() throws InterruptedException {
    String path = "/temp2";
    String loc = null;
    try {
      loc = hhc.createEphemeralNode(path);
      Thread.sleep(10000);
    } catch (HungryHippoException e) {
      assertTrue(false);// exception should not occur;
    }
    assertEquals(path, loc);
  }

  @Test
  public void testCreateEphemeralNode_2Arg() {
    String path = "/temp1";
    String data = "temp";
    String loc = null;
    try {
      loc = hhc.createEphemeralNode(path, data);
    } catch (HungryHippoException e) {
      assertTrue(false);// exception should not occur;
    }
    assertEquals(path, loc);
  }

  @Test
  public void testCreateEphemerealNodeSeq() {
    String path = "/temp_seq";
    String loc = null;
    try {
      loc = hhc.createEphemeralNodeSeq(path);
    } catch (HungryHippoException e) {
      assertTrue(false);// exception should not occur;
    }
    assertNotEquals(path, loc);
  }

  @Test
  public void testCreateEphemerealNodeSeq_2Arg() {
    String path = "/temp_seq_1";
    String data = "seq";
    String loc = null;
    try {
      loc = hhc.createEphemeralNodeSeq(path, data);
    } catch (HungryHippoException e) {
      assertTrue(false);// exception should not occur;
    }
    assertEquals(path, loc);
  }

  @Test
  public void testSetZnodeData() {
    String path = "/test1";
    String data = "setAStringData";
    Stat stat = null;
    try {
      stat = hhc.setZnodeData(path, data);
    } catch (HungryHippoException e) {
      assertTrue(false);// exception should not occur;
    }
    assertNotNull(stat);

  }


  @Test
  public void testSetZnodeData_Async() {
    String path = "/test1";
    String data = "madeanasynccall";
    Stat stat = null;
    try {
      stat = hhc.setZnodeDataAsync(path, data);
    } catch (HungryHippoException e) {
      assertTrue(false);// exception should not occur;
    }
    assertNotNull(stat);

  }

  @Test
  public void testCreateZnodeIfNotPresent() throws HungryHippoException {
    String path = "/test1/hi";
    long l = 55;
    hhc.createPersistentNodeIfNotPresent(path, l);
  }

  @Test
  public void testupdatePersistentNode() throws HungryHippoException {
    String path = "/test1/hello";
    long l = 95;
    hhc.updatePersistentNode(path, l);
  }
}
