package com.talentica.hungryHippos.sharding;

public enum ShardingTableFilesName {

  BUCKET_COMBINATION_TO_NODE_NUMBERS_MAP_FILE("bucketCombinationToNodeNumbersMap"),
  
  BUCKET_TO_NODE_NUMBER_MAP_FILE("bucketToNodeNumberMap"),
  
  KEY_TO_VALUE_TO_BUCKET_MAP_FILE("keyToValueToBucketMap");
  
  private String name;
  
  private ShardingTableFilesName(String name){
    this.name = name;
  }
  
  public String getName(){
    return name;
  }
  
}
