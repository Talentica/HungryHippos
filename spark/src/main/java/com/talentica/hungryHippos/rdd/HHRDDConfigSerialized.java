package com.talentica.hungryHippos.rdd;

import java.io.Serializable;
import java.util.List;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.rdd.utility.BroadcastVariable;

public class HHRDDConfigSerialized implements Serializable {

  
  private static final long serialVersionUID = -1970173579825061525L;
  private int rowSize;
  private int[] shardingIndexes;
  private String directoryLocation;
  private String shardingFolderPath;
  private List<SerializedNode> nodes;
  private FieldTypeArrayDataDescription fieldDataDesc;
  private BroadcastVariable broadcastVariable;

  public HHRDDConfigSerialized(int rowSize, int[] shardingIndexes, String directoryLocation,
      String shardingFolderPath, List<SerializedNode> nodes,
      FieldTypeArrayDataDescription fieldDataDesc,BroadcastVariable broadcastVariable) {
    super();
    this.rowSize = rowSize;
    this.shardingIndexes = shardingIndexes;
    this.directoryLocation = directoryLocation;
    this.shardingFolderPath = shardingFolderPath;
    this.nodes = nodes;
    this.fieldDataDesc = fieldDataDesc;
    this.broadcastVariable = broadcastVariable;
  }

  public int[] getShardingIndexes() {
    return shardingIndexes;
  }

  public String getDirectoryLocation() {
    return directoryLocation;
  }

  public String getShardingFolderPath() {
    return shardingFolderPath;
  }

  public List<SerializedNode> getNodes() {
    return nodes;
  }

  public int getRowSize() {
    return rowSize;
  }

  public FieldTypeArrayDataDescription getFieldTypeArrayDataDescription() {
    return this.fieldDataDesc;
  }

  public BroadcastVariable getBroadcastVariable() {
    return broadcastVariable;
  }

}
