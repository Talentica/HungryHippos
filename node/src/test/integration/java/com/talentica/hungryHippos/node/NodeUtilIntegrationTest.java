package com.talentica.hungryHippos.node;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.BucketCombination;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;

public class NodeUtilIntegrationTest {
  private NodesManager nodesManager;
  private Map<BucketCombination, Set<Node>> bucketCombinationToNodeNumbersMap =
      new HashMap<BucketCombination, Set<Node>>();
  private Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap =
      new HashMap<String, Map<Object, Bucket<KeyValueFrequency>>>();
  private Map<String, Map<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap =
      new HashMap<String, Map<Bucket<KeyValueFrequency>, Node>>();
  private String basePath = "/home/pooshans";
  private String clientConfig = basePath + "/HungryHippos/configuration-schema/src/main/resources/schema/client-config.xml";

  @Before
  public void setUp() throws Exception {
    nodesManager = NodesManagerContext.initialize(clientConfig);
  }

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testGetKeyToValueToBucketMap() {
    NodeUtil.getKeyToValueToBucketMap();
  }

  @Test
  public void testGetBucketToNodeNumberMap() {
    NodeUtil.getBucketToNodeNumberMap();
  }

  @Test
  public void testGetNodeId() {
    try {
      NodeUtil.getNodeId();
    } catch (IOException e) {
      assertTrue(false);
    }
  }

  @Test
  public void testCreateTrieBucketToNodeNumberMap() {
    try {
      NodeUtil.createTrieBucketToNodeNumberMap(bucketToNodeNumberMap, nodesManager);
    } catch (IOException e) {
      assertTrue(false);
    }
  }

  @Test
  public void testCreateTrieKeyToValueToBucketMap() {
    try {
      NodeUtil.createTrieKeyToValueToBucketMap(keyToValueToBucketMap, nodesManager);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Test
  public void testCreateTrieBucketCombinationToNodeNumbersMap() {
    try {
      NodeUtil.createTrieBucketCombinationToNodeNumbersMap(bucketCombinationToNodeNumbersMap,
          nodesManager);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
