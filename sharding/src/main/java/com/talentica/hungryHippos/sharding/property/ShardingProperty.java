package com.talentica.hungryHippos.sharding.property;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.property.Property;

/**
 * 
 * @author sohanc
 *
 */
public class ShardingProperty extends Property<ShardingProperty> {

	private static final Logger LOGGER = LoggerFactory.getLogger(ShardingProperty.class.getName());

	public ShardingProperty(String propFileName) {
		super(propFileName);
	}

	public ShardingProperty() {

	}
}
