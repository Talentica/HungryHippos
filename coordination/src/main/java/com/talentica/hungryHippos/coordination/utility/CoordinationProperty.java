/**
 * 
 */
package com.talentica.hungryHippos.coordination.utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.property.Property;


/**
 * @author pooshans
 *
 */
public class CoordinationProperty extends Property<CoordinationProperty> {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(CoordinationProperty.class.getName());

  public CoordinationProperty(String propFileName) {
    super(propFileName);
  }

  public CoordinationProperty() {

  }
  
  public final String[] getDataTypeConfiguration() {
    return getValueByKey("column.datatype-size").toString().split(",");
  }

  public final int getMaximumSizeOfSingleDataBlock() {
    return Integer.parseInt(getValueByKey("maximum.size.of.single.block.data"));
  }



}
