package com.talentica.hungryHippos.sharding.context;

import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.sharding.ShardingConfig;

/**
 * 
 * @author sohanc
 *
 */
public class ShardingApplicationContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShardingApplicationContext.class);

  private static ShardingConfig shardingConfig;

  public static ShardingConfig getZkShardingConfigCache() {
    if (shardingConfig != null) {
      return shardingConfig;
    }
    ZKNodeFile configurationFile;
    try {
      configurationFile =
          (ZKNodeFile) NodesManagerContext.getNodesManagerInstance().getConfigFileFromZNode(
              CoordinationApplicationContext.SHARDING_CONFIGURATION);
      ShardingConfig shardingConfig =
          JaxbUtil.unmarshal((String) configurationFile.getObj(), ShardingConfig.class);
      ShardingApplicationContext.shardingConfig = shardingConfig;
      return ShardingApplicationContext.shardingConfig;
    } catch (ClassNotFoundException | KeeperException | InterruptedException | IOException e) {
      LOGGER.info("Please upload the sharding configuration file on zookeeper");
    } catch (JAXBException e1) {
      LOGGER.info("Unable to unmarshal the sharding xml configuration.");
    }
    return shardingConfig;
  }
}
