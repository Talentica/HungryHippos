/**
 * 
 */
package com.talentica.hungryHippos.master.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.utility.CommonProperty;

/**
 * @author pooshans
 *
 */
public class DataPublisherProperty extends CommonProperty<DataPublisherProperty> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataPublisherProperty.class
      .getName());

  public DataPublisherProperty(String propFileName) {
    super(propFileName);
  }

  public DataPublisherProperty() {

  }

  public final String[] getDataTypeConfiguration() {
    return getValueByKey("column.datatype-size").toString().split(",");
  }

  public final int getMaximumSizeOfSingleDataBlock() {
    return Integer.parseInt(getValueByKey("maximum.size.of.single.block.data"));
  }
}
