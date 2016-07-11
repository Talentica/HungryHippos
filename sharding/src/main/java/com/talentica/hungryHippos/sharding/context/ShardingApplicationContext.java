package com.talentica.hungryHippos.sharding.context;

import java.io.FileNotFoundException;

import javax.xml.bind.JAXBException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.property.Property;
import com.talentica.hungryHippos.sharding.property.ShardingProperty;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.sharding.ShardingConfig;

/**
 * 
 * @author sohanc
 *
 */
public class ShardingApplicationContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShardingApplicationContext.class);

  private static Property<ShardingProperty> property;
  private static FieldTypeArrayDataDescription dataDescription;
  private static String shardingApplicationContext;

  public static Property<ShardingProperty> getProperty() {
    if (property == null) {
      property = new ShardingProperty("sharding-config.properties");
    }
    return property;
  }

  public static ShardingConfig getShardingConfig() throws FileNotFoundException, JAXBException {
    if (ShardingApplicationContext.shardingApplicationContext == null) {
      LOGGER.info("Please set the sharding configuration file path.");
      return null;
    }
    return JaxbUtil.unmarshalFromFile(ShardingApplicationContext.shardingApplicationContext,
        ShardingConfig.class);
  }

  public static void setShardingConfigPathContext(String shardingConfigFilePath) {
    ShardingApplicationContext.shardingApplicationContext = shardingConfigFilePath;
  }
}
