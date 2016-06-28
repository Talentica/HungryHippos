package com.talentica.hungryHippos.sharding.context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.property.Property;
import com.talentica.hungryHippos.sharding.property.ShardingProperty;

/**
 * 
 * @author sohanc
 *
 */
public class ShardingApplicationContext {

	private static final Logger LOGGER = LoggerFactory
		      .getLogger(ShardingApplicationContext.class);
	
	private static Property<ShardingProperty> property;
	private static FieldTypeArrayDataDescription dataDescription;
	
	public static Property<ShardingProperty> getProperty(){
		if(property == null){
			property = new ShardingProperty("sharding-config.properties");
		}
		return property;
	}
}
