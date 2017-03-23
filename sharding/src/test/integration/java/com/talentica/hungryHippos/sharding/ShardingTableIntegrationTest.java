/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
/**
 * 
 */
package com.talentica.hungryHippos.sharding;

import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;

/**
 * @author pooshans
 * @author sohanc
 */
public class ShardingTableIntegrationTest {
  private ShardingTableZkService shardingTable;
  private static final String shardingPath = "/dir/dir1/input";
  private HungryHippoCurator curator;

  @Before
  public void setUp() throws Exception {
    shardingTable = new ShardingTableZkService();
    
  }

  @Test
  @Ignore
  public void testBucketCombinationToNode() {
    try {
      shardingTable.zkUploadBucketCombinationToNodeNumbersMap(shardingPath);
      Assert.assertTrue(true);
    } catch (IllegalArgumentException | IllegalAccessException | IOException
        | InterruptedException | HungryHippoException e) {
      Assert.assertFalse(true);
    }
  }

  @Test
  @Ignore
  public void testBucketToNodeNumber()
      throws IllegalArgumentException, IllegalAccessException, IOException, InterruptedException {
    try {
      shardingTable.zkUploadBucketToNodeNumberMap(shardingPath);
      Assert.assertTrue(true);
    } catch (IllegalArgumentException | IllegalAccessException | IOException
        | InterruptedException | HungryHippoException e) {
      Assert.assertFalse(true);
    }
  }

  @Test
  @Ignore
  public void testKeyToValueToBucket() throws IllegalArgumentException, IllegalAccessException,
      IOException, InterruptedException, JAXBException {
    try {
      shardingTable.zkUploadKeyToValueToBucketMap(shardingPath);
      Assert.assertTrue(true);
    } catch (IllegalArgumentException | IllegalAccessException | IOException
        | InterruptedException | HungryHippoException e) {
      Assert.assertFalse(true);
    }
  }

  @Test
  @Ignore
  public void testBucketCombinationToNodeRead() {
    try {
      shardingTable.readBucketCombinationToNodeNumbersMap(shardingPath);
      Assert.assertTrue(true);
    } catch (Exception e) {
      Assert.assertFalse(true);
    }
  }

  @Test
  @Ignore
  public void testBucketToNodeNumberRead() {
    try {
      shardingTable.readBucketToNodeNumberMap(shardingPath);
      Assert.assertTrue(true);
    } catch (Exception e) {
      Assert.assertFalse(true);
    }
  }

  @Test
  public void testKeyToValueToBucketRead() {
    try {
      shardingTable.readKeyToValueToBucketMap(shardingPath);
      Assert.assertTrue(true);
    } catch (Exception e) {
      Assert.assertFalse(true);
    }
  }

  @Test
  public void testBucketToNodeNumberObject() throws Exception {
    try {
      curator.createPersistentNode("/rootnode/test",
          shardingTable.getKeyToValueToBucketMap());
      Assert.assertTrue(true);
    } catch (IllegalArgumentException e) {
      Assert.assertFalse(true);
    }
  }

  @Test
  public <V, K> void testGetBucketToNodeNumberObject() throws Exception {
    Object object = curator.readObject("/rootnode/test");
    Assert.assertNotNull(object);
  }
}
