/**
 * 
 */
package com.talentica.hungryHippos.rdd;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBException;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.rdd.utility.JaxbUtil;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.sharding.util.ShardingTableCopier;
import com.talentica.hungryhippos.config.client.ClientConfig;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.config.cluster.Node;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;


/**
 * @author pooshans
 */
public class HHRDDConfiguration implements Serializable {

  private static final long serialVersionUID = -9079703351777187673L;
  private int rowSize;
  private int[] shardingIndexes;
  private String directoryLocation;
  private FieldTypeArrayDataDescription dataDescription;
  private ClientConfig clientConfig;
  private ClusterConfig clusterConfig;
  private String shardingFolderPath;
  private List<Node> nodes;
  private List<SerializedNode> serializedNodes = new ArrayList<>();

  public HHRDDConfiguration(String distributedPath, String clientConfigPath)
      throws FileNotFoundException, JAXBException {
    initialize(distributedPath, clientConfigPath);
    ShardingApplicationContext context = new ShardingApplicationContext(shardingFolderPath);
    this.dataDescription = context.getConfiguredDataDescription();
    this.rowSize = dataDescription.getSize();
    this.shardingIndexes = context.getShardingIndexes();
    this.clusterConfig = CoordinationConfigUtil.getZkClusterConfigCache();
    nodes = this.clusterConfig.getNode();
  }

  private void initialize(String distributedPath, String clientConfigPath)
      throws JAXBException, FileNotFoundException {
    this.clientConfig = JaxbUtil.unmarshalFromFile(clientConfigPath, ClientConfig.class);
    String servers = clientConfig.getCoordinationServers().getServers();
    HungryHippoCurator.getInstance(servers);
    this.shardingFolderPath = FileSystemContext.getRootDirectory()+ distributedPath
        + HungryHippoCurator.ZK_PATH_SEPERATOR + ShardingTableCopier.SHARDING_ZIP_FILE_NAME;
    this.directoryLocation =
        FileSystemContext.getRootDirectory() + distributedPath + HungryHippoCurator.ZK_PATH_SEPERATOR + "data_";
  }

  public List<SerializedNode> getNodes() {
    for (Node node : nodes) {
      serializedNodes.add(new SerializedNode(node.getIdentifier(), node.getIp()));
    }
    return serializedNodes;
  }

  public String getShardingFolderPath() {
    return this.shardingFolderPath;
  }

  public int getRowSize() {
    return rowSize;
  }

  public void setRowSize(int rowSize) {
    this.rowSize = rowSize;
  }

  public int[] getShardingIndexes() {
    return shardingIndexes;
  }

  public void setShardingIndexes(int[] shardingIndexes) {
    this.shardingIndexes = shardingIndexes;
  }

  public String getDirectoryLocation() {
    return directoryLocation;
  }

  public void setDirectoryLocation(String directoryLocation) {
    this.directoryLocation = directoryLocation;
  }

  public FieldTypeArrayDataDescription getDataDescription() {
    return this.dataDescription;
  }

}
