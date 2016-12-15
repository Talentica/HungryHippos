package com.talentica.hadoop.spark.job;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;

public class DataDescriptionConfig {

  private int rowSize;
  private FieldTypeArrayDataDescription dataDescription;
  
  public DataDescriptionConfig(String shardingFolderPath){
    ShardingApplicationContext context = new ShardingApplicationContext(shardingFolderPath);
    dataDescription = context.getConfiguredDataDescription();
    rowSize = dataDescription.getSize();
  }

  public int getRowSize() {
    return rowSize;
  }

  public FieldTypeArrayDataDescription getDataDescription() {
    return dataDescription;
  }

}
