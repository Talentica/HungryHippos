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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.coordination.utility.ZkNodeName;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.utility.PathUtil;

/**
 * {@code ShardingTableZkService } used for storing sharding table details on zookeeper.
 * 
 * @author pooshans
 * @author sohanc
 *
 */
public class ShardingTableZkService {
  private final Logger LOGGER = LoggerFactory.getLogger(ShardingTableZkService.class.getName());
  private Map<BucketCombination, Set<Node>> bucketCombinationToNodeNumbersMap = new HashMap<>();
  private Map<String, Map<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap = new HashMap<>();
  private Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap =
      new HashMap<>();
  private HungryHippoCurator curator = null;

  public ShardingTableZkService() {
    curator = HungryHippoCurator.getInstance();
  }

  /**
   * To upload the BucketCombinationNodeNumber to ZK. .
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   */
  public void zkUploadBucketCombinationToNodeNumbersMap(String path) throws IOException,
      InterruptedException, IllegalArgumentException, IllegalAccessException, HungryHippoException {
    curator.createPersistentNode(path + File.separatorChar + ZkNodeName.SHARDING_TABLE.getName()
        + File.separatorChar + ZkNodeName.BUCKET_COMBINATION.getName(),
        getBucketCombinationToNodeNumbersMap());
  }

  /**
   * To upload the BucketNodeNumber to ZK.
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   */
  public void zkUploadBucketToNodeNumberMap(String path) throws IOException, InterruptedException,
      IllegalArgumentException, IllegalAccessException, HungryHippoException {
    curator.createPersistentNode(path + File.separatorChar + ZkNodeName.SHARDING_TABLE.getName()
        + File.separatorChar + ZkNodeName.KEY_TO_BUCKET_NUMBER.getName(),
        getBucketToNodeNumberMap());
  }

  /**
   * To upload the KeyValueBucket to ZK.
   * 
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   * @throws IOException
   * @throws InterruptedException
   */
  public void zkUploadKeyToValueToBucketMap(String path) throws IllegalArgumentException,
      IllegalAccessException, IOException, InterruptedException, HungryHippoException {
    curator.createPersistentNode(path + File.separatorChar + ZkNodeName.SHARDING_TABLE.getName()
        + File.separatorChar + ZkNodeName.KEY_TO_VALUE_TO_BUCKET.getName(),
        getKeyToValueToBucketMap());
  }

  /**
   * @return Map<String, Map<Object, Bucket<KeyValueFrequency>>>
   */
  @SuppressWarnings("unchecked")
  public Map<String, Map<Object, Bucket<KeyValueFrequency>>> getKeyToValueToBucketMap() {
    try (ObjectInputStream inKeyValueNodeNumberMap = new ObjectInputStream(
        new FileInputStream(new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath()
            + PathUtil.SEPARATOR_CHAR + ShardingApplicationContext.keyToValueToBucketMapFile))) {
      return (Map<String, Map<Object, Bucket<KeyValueFrequency>>>) inKeyValueNodeNumberMap
          .readObject();
    } catch (IOException | ClassNotFoundException e) {
      LOGGER.info("Unable to read keyValueNodeNumberMap. Please put the file in current directory");
    }
    return null;
  }

  /**
   * @return Map<String, Map<Bucket<KeyValueFrequency>, Node>>
   */
  @SuppressWarnings("unchecked")
  public Map<String, Map<Bucket<KeyValueFrequency>, Node>> getBucketToNodeNumberMap() {
    try (ObjectInputStream bucketToNodeNumberMapInputStream = new ObjectInputStream(
        new FileInputStream(new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath()
            + PathUtil.SEPARATOR_CHAR + ShardingApplicationContext.bucketToNodeNumberMapFile))) {
      return (Map<String, Map<Bucket<KeyValueFrequency>, Node>>) bucketToNodeNumberMapInputStream
          .readObject();
    } catch (IOException | ClassNotFoundException e) {
      LOGGER.info("Unable to read bucketToNodeNumberMap. Please put the file in current directory");
    }
    return null;
  }

  /**
   * @return Map<BucketCombination, Set<Node>>
   */
  @SuppressWarnings("unchecked")
  public Map<BucketCombination, Set<Node>> getBucketCombinationToNodeNumbersMap() {
    try (ObjectInputStream bucketCombinationToNodeNumbersMapStream =
        new ObjectInputStream(new FileInputStream(
            new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath() + PathUtil.SEPARATOR_CHAR
                + ShardingApplicationContext.bucketCombinationToNodeNumbersMapFile))) {
      return (Map<BucketCombination, Set<Node>>) bucketCombinationToNodeNumbersMapStream
          .readObject();
    } catch (IOException | ClassNotFoundException e) {
      LOGGER.info(
          "Unable to read bucketCombinationToNodeNumbersMap. Please put the file in current directory");
    }
    return null;
  }

  /**
   * @return Map<BucketCombination, Set<Node>>
   */
  @SuppressWarnings("unchecked")
  public Map<BucketCombination, Set<Node>> readBucketCombinationToNodeNumbersMap(
      String shardingTablePath) {
    String bucketCombinationPath = CoordinationConfigUtil.getFileSystemPath() + shardingTablePath + File.separatorChar
        + ZkNodeName.SHARDING_TABLE.getName() + File.separatorChar
        + ZkNodeName.BUCKET_COMBINATION.getName();
    try {
      bucketCombinationToNodeNumbersMap =
          (Map<BucketCombination, Set<Node>>) curator.readObject(bucketCombinationPath);
    } catch (HungryHippoException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return bucketCombinationToNodeNumbersMap;
  }

  /**
   * @return Map<String, Map<Bucket<KeyValueFrequency>, Node>>
   */
  @SuppressWarnings("unchecked")
  public Map<String, Map<Bucket<KeyValueFrequency>, Node>> readBucketToNodeNumberMap(
      String shardingTablePath) {
    String keyToBucketNumberPath = CoordinationConfigUtil.getFileSystemPath() + shardingTablePath + File.separatorChar
        + ZkNodeName.SHARDING_TABLE.getName() + File.separatorChar
        + ZkNodeName.KEY_TO_BUCKET_NUMBER.getName();
    try {
      bucketToNodeNumberMap = (Map<String, Map<Bucket<KeyValueFrequency>, Node>>) curator
          .readObject(keyToBucketNumberPath);
    } catch (HungryHippoException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return bucketToNodeNumberMap;
  }

  /**
   * @return Map<String, Map<Object, Bucket<KeyValueFrequency>>>
   */
  @SuppressWarnings("unchecked")
  public Map<String, Map<Object, Bucket<KeyValueFrequency>>> readKeyToValueToBucketMap(
      String shardingTablePath) {
    String KeyToValueToBucketPath = CoordinationConfigUtil.getFileSystemPath() + shardingTablePath + File.separatorChar
        + ZkNodeName.SHARDING_TABLE.getName() + File.separatorChar
        + ZkNodeName.KEY_TO_VALUE_TO_BUCKET.getName();
    try {
      keyToValueToBucketMap = (Map<String, Map<Object, Bucket<KeyValueFrequency>>>) curator
          .readObject(KeyToValueToBucketPath);
    } catch (HungryHippoException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return keyToValueToBucketMap;
  }
}
