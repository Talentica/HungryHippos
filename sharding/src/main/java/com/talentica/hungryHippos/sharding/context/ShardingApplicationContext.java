package com.talentica.hungryHippos.sharding.context;

import java.io.FileNotFoundException;

import javax.xml.bind.JAXBException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.sharding.ShardingConfig;

/**
 * 
 * @author sohanc
 *
 */
public class ShardingApplicationContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShardingApplicationContext.class);

  private static FieldTypeArrayDataDescription dataDescription;
  private static String shardingApplicationContext;
  private static ShardingConfig shardingConfig;

  public static ShardingConfig getShardingConfig() throws FileNotFoundException, JAXBException {
    if (shardingConfig != null) {
      return shardingConfig;
    }
    if (ShardingApplicationContext.shardingApplicationContext == null) {
      LOGGER.info("Please set the sharding configuration file path.");
      return null;
    }
    shardingConfig =
        JaxbUtil.unmarshalFromFile(ShardingApplicationContext.shardingApplicationContext,
            ShardingConfig.class);
    return shardingConfig;
  }

  public static void setShardingConfigPathContext(String shardingConfigFilePath) {
    ShardingApplicationContext.shardingApplicationContext = shardingConfigFilePath;
  }
}
