/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *******************************************************************************/
package com.talentica.hungryHippos.sharding.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryhippos.config.sharding.Column;

public class ShardingFileUtil {

  private static Logger LOGGER = LoggerFactory.getLogger(ShardingFileUtil.class);

  public static void dumpKeyToValueToBucketFileOnDisk(String fileName,
      HashMap<String, HashMap<Object, Bucket<KeyValueFrequency>>> keyToValueToBucket,
      String folderPath) throws IOException {
    new File(folderPath).mkdirs();
    String filePath = (folderPath.endsWith(String.valueOf("/")) ? folderPath + fileName
        : folderPath + "/" + fileName);
    File file = new File(filePath);
    FileOutputStream output = null;
    ObjectOutputStream oos = null;
    try {
      output = new FileOutputStream(file);
      oos = new ObjectOutputStream(output);
      oos.writeObject(keyToValueToBucket);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        closeOutputStream(output, oos);
      } catch (IOException e) {
        e.printStackTrace();
        throw e;
      }
    }
  }

  public static void dumpBucketToNodeNumberFileOnDisk(String fileName,
      HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumber,
      String folderPath) throws IOException {
    new File(folderPath).mkdirs();
    String filePath = (folderPath.endsWith(String.valueOf("/")) ? folderPath + fileName
        : folderPath + "/" + fileName);
    File file = new File(filePath);
    FileOutputStream output = null;
    ObjectOutputStream oos = null;
    try {
      output = new FileOutputStream(file);
      oos = new ObjectOutputStream(output);
      oos.writeObject(bucketToNodeNumber);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        closeOutputStream(output, oos);
      } catch (IOException e) {
        e.printStackTrace();
        throw e;
      }
    }
  }

  private static void closeOutputStream(FileOutputStream output, ObjectOutputStream oos)
      throws IOException {
    if (oos != null) {
      oos.flush();
      oos.close();
    }
    if (output != null) {
      output.close();
    }
  }

  @SuppressWarnings("unchecked")
  public static HashMap<String, HashMap<Object, Bucket<KeyValueFrequency>>> readFromFileKeyToValueToBucket(
      String filePath, Map<String, String> dataTypeMap) {
    LOGGER.info(" sharding filePath : " + filePath);
    File file = new File(filePath);
    FileInputStream fis = null;
    ObjectInputStream ois = null;
    HashMap<String, HashMap<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap = null;
    try {
      fis = new FileInputStream(file);
      ois = new ObjectInputStream(fis);
      keyToValueToBucketMap =
          (HashMap<String, HashMap<Object, Bucket<KeyValueFrequency>>>) ois.readObject();
    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
    } finally {
      try {
        closeInputStream(fis, ois);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    return keyToValueToBucketMap;
  }

  private static void closeInputStream(FileInputStream fis, ObjectInputStream ois)
      throws IOException {
    if (ois != null) {
      ois.close();
    }
    if (fis != null) {
      fis.close();
    }
  }

  @SuppressWarnings("unchecked")
  public static HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> readFromFileBucketToNodeNumber(
      String filePath) {
    LOGGER.info(" sharding filePath : " + filePath);
    File file = new File(filePath);
    FileInputStream fis = null;
    ObjectInputStream ois = null;
    HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap = null;
    try {
      fis = new FileInputStream(file);
      ois = new ObjectInputStream(fis);
      bucketToNodeNumberMap =
          (HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>>) ois.readObject();
    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
    } finally {
      try {
        closeInputStream(fis, ois);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return bucketToNodeNumberMap;
  }


  public static Map<String, String> getDataTypeMap(ShardingApplicationContext context) {
    Map<String, String> dataTypeMap = new HashMap<>();
    List<Column> columns =
        context.getShardingClientConfig().getInput().getDataDescription().getColumn();
    for (Column column : columns) {
      dataTypeMap.put(column.getName(), column.getDataType());
    }
    return dataTypeMap;
  }

}
