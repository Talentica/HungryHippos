/**
 * 
 */
package com.talentica.hungryHippos.rdd;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.rdd.utility.JaxbUtil;
import com.talentica.hungryHippos.rdd.utility.ShardingApplicationContext;
import com.talentica.hungryhippos.config.client.ClientConfig;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.config.cluster.Node;


/**
 * @author pooshans
 *
 */
public class HHRDDConf implements Serializable {

  private static final long serialVersionUID = -9079703351777187673L;
  private int rowSize;
  private int[] shardingIndexes;
  private String directoryLocation;
  private DataDescription dataDescription;
  private ClientConfig clientConfig;
  private ClusterConfig clusterConfig;
  private String shardingFolderPath;
  private Map<Integer, String> nodeLookUp = new HashMap<>();

  public HHRDDConf(String shardingFolderPath, String directoryLocation, String clientConfigPath)
      throws FileNotFoundException, JAXBException {
    this.shardingFolderPath = shardingFolderPath;
    this.directoryLocation = directoryLocation;

    ShardingApplicationContext context = new ShardingApplicationContext(shardingFolderPath);
    this.dataDescription = context.getConfiguredDataDescription();
    this.rowSize = dataDescription.getSize();
    this.shardingIndexes = context.getShardingIndexes();
    this.clientConfig = JaxbUtil.unmarshalFromFile(clientConfigPath, ClientConfig.class);
    String servers = clientConfig.getCoordinationServers().getServers();
    HungryHippoCurator.getInstance(servers);
    this.clusterConfig = CoordinationConfigUtil.getZkClusterConfigCache();

    List<Node> nodes = this.clusterConfig.getNode();
    for (Node node : nodes) {
      nodeLookUp.put(node.getIdentifier(), node.getIp());
    }
  }


  public String getShardingFolderPath() {
    return this.shardingFolderPath;
  }

  public Map<Integer, String> getNodeLookUp() {
    return this.nodeLookUp;
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

  public DataDescription getDataDescription() {
    return this.dataDescription;
  }

}
