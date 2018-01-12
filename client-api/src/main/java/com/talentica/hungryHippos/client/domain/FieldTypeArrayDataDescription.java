/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.talentica.hungryHippos.client.domain;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * 
 * The system uses this to decode the values of encrypted file. The content of the file is read in
 * byte[]. System uses this class to convert the byte content into human readable format.
 * 
 * @author debasishc
 * @since 1/9/15.
 */
public final class FieldTypeArrayDataDescription
    implements DataDescription, Serializable, Cloneable {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FieldTypeArrayDataDescription.class.getName());

  private int maximumSizeOfSingleBlockOfData = 0;

  private static final long serialVersionUID = 1551866958938738882L;
  private Map<Integer, DataLocator> dataLocatorMap = new HashMap<>();
  private int nextIndex = 0;
  private int nextOffset = 0;

  public void setKeyOrder(String[] keyOrder) {
    this.keyOrder = keyOrder;
  }

  /**
   * Creates a new FieldTypeArrayDataDescription with a defined size of array. In case of variable
   * line length in a file always choose maximum size of the line. Because once defined it. It can't
   * be changed.
   * 
   * @param maximumSizeOfSingleBlockOfData is the maximum size of a row.
   */
  public FieldTypeArrayDataDescription(int maximumSizeOfSingleBlockOfData) {
    this.maximumSizeOfSingleBlockOfData = maximumSizeOfSingleBlockOfData;
  }

  private String[] keyOrder;

  @Override
  public DataLocator locateField(int index) {
    return dataLocatorMap.get(index);
  }

  @Override
  public int getSize() {
    return nextOffset;
  }

  @Override
  public String[] keyOrder() {
    return keyOrder;
  }

  /**
   * This method is used for adding fieldType details. Populates DataType and the location of that
   * DataType in the array.
   * 
   * @param dataType is an enumeration value used for representing type o data it is. i.e float or
   *        string and so on.
   * @param size is the size of each @see DataLocator.DataType in bytes. i.e
   *        DataLocator.DataType.FLOAT takes 4 bytes.
   */
  public void addFieldType(DataLocator.DataType dataType, int size) {

    switch (dataType) {
      case BYTE:
        size = Byte.BYTES;
        break;
      case SHORT:
        size = Short.BYTES;
        break;
      case INT:
        size = Integer.BYTES;
        break;
      case LONG:
        size = Long.BYTES;
        break;
      case DATE:
        size = Long.BYTES;
        break;
      case TIMESTAMP:
        size = Long.BYTES;
        break;
      case FLOAT:
        size = Float.BYTES;
        break;
      case DOUBLE:
        size = Double.BYTES;
        break;
    }
    DataLocator locator = new DataLocator(dataType, nextOffset, size);
    dataLocatorMap.put(nextIndex, locator);
    nextIndex++;
    nextOffset += size;
  }

  /**
   * Used for creating a new FieldTypeDataDescription.
   * 
   * @param dataTypeConfiguration is an array containing data type for each field.
   * @param totalSize is the max(size) of a single row.
   * @return a FieldTypeArrayDataDescription.
   */
  public static final FieldTypeArrayDataDescription createDataDescription(
      String[] dataTypeConfiguration, int totalSize) {
    FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription(totalSize);
    String[] datatype_size;
    String datatype;
    if (dataTypeConfiguration == null || dataTypeConfiguration.length == 0) {
      LOGGER.warn("\n\t No data type is specified in the configuration file");
      return dataDescription;
    }
    for (int index = 0; index < dataTypeConfiguration.length; index++) {
      int size = 0;
      datatype_size = dataTypeConfiguration[index].split("-");
      datatype = datatype_size[0];
      if (datatype_size.length > 1) {
        size = Integer.valueOf(datatype_size[1]);
      }
      switch (datatype) {
        case "STRING":
          dataDescription.addFieldType(DataLocator.DataType.STRING, size);
          break;
        case "DOUBLE":
          dataDescription.addFieldType(DataLocator.DataType.DOUBLE, size);
          break;
        case "FLOAT":
          dataDescription.addFieldType(DataLocator.DataType.FLOAT, size);
          break;
        case "TIMESTAMP":
          dataDescription.addFieldType(DataLocator.DataType.TIMESTAMP, size);
          break;
        case "LONG":
          dataDescription.addFieldType(DataLocator.DataType.LONG, size);
          break;
        case "INT":
          dataDescription.addFieldType(DataLocator.DataType.INT, size);
          break;
        case "DATE":
          dataDescription.addFieldType(DataLocator.DataType.DATE, size);
          break;
        case "SHORT":
          dataDescription.addFieldType(DataLocator.DataType.SHORT, size);
          break;
        case "BYTE":
          dataDescription.addFieldType(DataLocator.DataType.BYTE, size);
          break;
        default:
          LOGGER.info("\n\t {} datatype is undefined", datatype);
      }
    }
    return dataDescription;
  }

  @Override
  public FieldTypeArrayDataDescription clone() {
    FieldTypeArrayDataDescription dataDescription =
        new FieldTypeArrayDataDescription(maximumSizeOfSingleBlockOfData);
    if (dataLocatorMap != null) {
      dataDescription.dataLocatorMap.putAll(dataLocatorMap);
      dataDescription.nextIndex = nextIndex;
      dataDescription.nextOffset = nextOffset;
      dataDescription.keyOrder = keyOrder;
    }
    return dataDescription;
  }

  @Override
  public int getNumberOfDataFields() {
    return dataLocatorMap.size();
  }

  @Override
  public int getMaximumSizeOfSingleBlockOfData() {
    return maximumSizeOfSingleBlockOfData;
  }

}
