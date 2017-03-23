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
package com.talentica.hdfs.spark.binary.job;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryhippos.config.sharding.ShardingClientConfig;

import javax.xml.bind.JAXBException;
import java.io.FileNotFoundException;

public class DataDescriptionConfig {

  private int rowSize;
  private FieldTypeArrayDataDescription dataDescription;
  private ShardingClientConfig shardingClientConfig;

  public DataDescriptionConfig(String shardingFolderPath) throws JAXBException, FileNotFoundException {
    ShardingApplicationContext context = new ShardingApplicationContext(shardingFolderPath);
    dataDescription = context.getConfiguredDataDescription();
    rowSize = dataDescription.getSize();
    shardingClientConfig = context.getShardingClientConfig();
  }

  public int getRowSize() {
    return rowSize;
  }

  public FieldTypeArrayDataDescription getDataDescription() {
    return dataDescription;
  }

  public ShardingClientConfig getShardingClientConf() {
    return shardingClientConfig;
  }

}
