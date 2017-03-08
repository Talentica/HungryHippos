package com.talentica.hdfs.spark.binary.job;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryhippos.config.sharding.ShardingClientConfig;

public class DataDescriptionConfig {

  private int rowSize;
  private FieldTypeArrayDataDescription dataDescription;
  private ShardingClientConfig shardingClientConfig;

  public DataDescriptionConfig(String shardingFolderPath) {
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