/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
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
package com.talentica.hungryHippos.node.service;

import com.talentica.hungryHippos.client.data.parser.DataParser;
import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataTypes;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.InvalidRowException;
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.coordination.utility.marshaling.FileWriter;
import com.talentica.hungryHippos.coordination.utility.marshaling.Reader;
import com.talentica.hungryHippos.node.datareceiver.ShardingResourceCache;
import com.talentica.hungryHippos.node.storage.strategy.StorageStrategy;
import com.talentica.hungryHippos.node.storage.strategy.StorageStrategyFunctionPool;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.BucketsCalculator;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.storage.StoreType;
import com.talentica.hungryHippos.utility.Counter;
import com.talentica.hungryhippos.config.sharding.DataParserConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by rajkishoreh on 23/11/16.
 */
public class DataDistributor {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataDistributor.class);

  public static void distribute(String hhFilePath, String srcDataPath) throws Exception {
    try{
    String BAD_RECORDS_FILE = srcDataPath + "_distributor.err";
    ShardingApplicationContext context = ShardingResourceCache.INSTANCE.getContext(hhFilePath);
    FieldTypeArrayDataDescription dataDescription = context.getConfiguredDataDescription();
    byte[] buf = new byte[dataDescription.getSize()];
    ByteBuffer byteBuffer = ByteBuffer.wrap(buf);

    DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);

    String[] keyOrder = context.getShardingDimensions();

    HashMap<String, HashMap<String, List<Bucket<KeyValueFrequency>>>> keyToValueToBucketMap =
        ShardingResourceCache.INSTANCE.getKeyToValueToBucketMap(hhFilePath);

    HashMap<String, HashMap<String, Integer>> splittedKeyValueMap =
        ShardingResourceCache.INSTANCE.getSplittedKeyValueMap(hhFilePath);

    HashMap<String, HashMap<String, Counter>> splitKeyValueCounter = new HashMap<>();
    for(Map.Entry<String, HashMap<String, List<Bucket<KeyValueFrequency>>>> keyToValueToBucketEntry : keyToValueToBucketMap.entrySet()){
      String key = keyToValueToBucketEntry.getKey();
      HashMap<String, Counter> valueSplitCounter = new HashMap<>();
      splitKeyValueCounter.put(key, valueSplitCounter);
      for(Map.Entry<String, List<Bucket<KeyValueFrequency>>> valueToBucketEntry : keyToValueToBucketEntry.getValue().entrySet()) {
        Integer splitCount = splittedKeyValueMap.get(key).get(valueToBucketEntry.getKey());
        if(splitCount==null){
          valueSplitCounter.put(valueToBucketEntry.getKey(),new Counter(0));
        }else{
          valueSplitCounter.put(valueToBucketEntry.getKey(),new Counter(splitCount-1));
        }
      }

    }

    BucketsCalculator bucketsCalculator = new BucketsCalculator(keyToValueToBucketMap, context);


    File srcFile = new File(srcDataPath);

    DataParserConfig dataParserConfig = context.getShardingClientConfig().getInput().getDataParserConfig();
    String dataParserClassName =
        dataParserConfig.getClassName();
    DataParser dataParser = (DataParser) Class.forName(dataParserClassName)
        .getConstructor(DataDescription.class,char.class).newInstance(context.getConfiguredDataDescription(),
                    dataParserConfig.getDelimiter().charAt(0));


    LOGGER.info("\n\tDISTRIBUTION OF DATA ACROSS THE NODES STARTED... for {}", hhFilePath);

    if (srcFile.exists()) {
      StoreType storeType = StoreType.NODEWISEDATASTORE;
      StorageStrategy storageStrategy = StorageStrategyFunctionPool.INSTANCE.getStore(storeType);
      HHFileMapper hhFileMapper = new HHFileMapper(hhFilePath,  dataDescription.getSize(),
          keyOrder.length, storeType);
      Reader input = new com.talentica.hungryHippos.coordination.utility.marshaling.FileReader(
          srcDataPath, dataParser);
      int lineNo = 0;
      FileWriter fileWriter = new FileWriter(BAD_RECORDS_FILE);
      fileWriter.openFile();

      int[] buckets = new int[keyOrder.length];
      int maxBucketSize =
          Integer.parseInt(context.getShardingServerConfig().getMaximumNoOfShardBucketsSize());
      String key;
      int keyIndex;
      DataTypes[] parts;
      while (true) {
        try {
          parts = input.read();
        } catch (InvalidRowException e) {
          fileWriter.flushData(lineNo++, e);
          continue;
        }
        if (parts == null) {
          input.close();
          break;
        }

        for (int i = 0; i < keyOrder.length; i++) {
          key = keyOrder[i];
          keyIndex = context.assignShardingIndexByName(key);
          DataTypes value = parts[keyIndex];
          String valueStr = value.toString();
          Counter counter = splitKeyValueCounter.get(key).get(valueStr);
          if( counter != null){
            buckets[i] = bucketsCalculator.getBucketNumberForValue(key, valueStr,counter.getNextCount());
          }else{
            buckets[i] = bucketsCalculator.calculateBucketNumberForNewValue(key, valueStr);
          }
        }

        for (int i = 0; i < dataDescription.getNumberOfDataFields(); i++) {
          dynamicMarshal.writeValue(i, parts[i], byteBuffer);
        }
        storageStrategy.store(buf,hhFileMapper, buckets, maxBucketSize);
      }
      srcFile.delete();
      hhFileMapper.sync();
      fileWriter.close();
    }
    }finally {
      ShardingResourceCache.INSTANCE.releaseContext(hhFilePath);
    }
  }

}
