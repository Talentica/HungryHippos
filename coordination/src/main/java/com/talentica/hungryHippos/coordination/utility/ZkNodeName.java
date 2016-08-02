/**
 * 
 */
package com.talentica.hungryHippos.coordination.utility;

/**
 * @author pooshans
 *
 */
public enum ZkNodeName {
  
  SHARDING_TABLE("sharding-table"),

  BUCKET_COMBINATION("bucket_combination"),

  KEY_TO_BUCKET_NUMBER("key_to_bucket_number"),

  KEY_TO_VALUE_TO_BUCKET("key_to_value_to_bucket"),

  KEY_TO_BUCKET("key_to_bucket"),

  NODES("nodes"),

  BUCKET("bucket"),

  ID("id"),

  UNDERSCORE("_"),

  NODE("node"),

  EQUAL("=");

  private String name;

  private ZkNodeName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

}
