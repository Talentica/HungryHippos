/**
 * 
 */
package com.talentica.hungryHippos.rdd;

import java.io.FileNotFoundException;
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
public class CustomHHJobConfiguration {

  private String distributedPath;
  private String clientConfigPat;
  private String outputFileName;

  public CustomHHJobConfiguration(String distributedPath, String clientConfigPat, String outputFileName) {
    this.distributedPath = distributedPath;
    this.clientConfigPat = clientConfigPat;
    this.outputFileName = outputFileName;
  }

  public String getDistributedPath() {
    return distributedPath;
  }

  public String getClientConfigPat() {
    return clientConfigPat;
  }

  public String getOutputFileName() {
    return outputFileName;
  }
}
