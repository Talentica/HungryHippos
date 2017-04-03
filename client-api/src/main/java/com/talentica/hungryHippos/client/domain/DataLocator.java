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

/**
 * This class provides the description of the particular data.
 * 
 * @author debasishc.
 * @since 2015-09-01
 * @version 0.5.0
 */
public class DataLocator implements Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = -3265231048634949676L;

  public static enum DataType {
    BYTE, SHORT, INT, LONG, CHAR, FLOAT, DOUBLE, STRING;

  }

  private int offset;
  private int size;
  private DataType dataType;

  /**
   * Constructor of the DataLocator.
   * 
   * @param dataType is the type of data such as string, int, long etc.
   * @param offset is the position of the data.
   * @param size is the size of the data.
   */
  public DataLocator(DataType dataType, int offset, int size) {
    this.dataType = dataType;
    this.offset = offset;
    this.size = size;
  }

  /**
   * To get the enumeration of the DataType.
   * 
   * @return DataType enumeration is type of data such as string, int etc.
   */
  public DataType getDataType() {
    return dataType;
  }

  /**
   * To get the offset of the data or value.
   * 
   * @return offset is the position of the data.
   */
  public int getOffset() {
    return offset;
  }

  /**
   * To get the size of the data.
   * 
   * @return size is the data type size.
   */
  public int getSize() {
    return size;
  }
}
