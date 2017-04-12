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
 * This interface is written so that the client can create own class and do the processing.
 * 
 * @author sudarshans
 *
 */
public interface DataTypes extends Comparable<DataTypes>, Cloneable, Serializable {

  /**
   * for getting the length of underlying array.
   * 
   * @return the length of the array.
   */
  int getLength();

  /**
   * for getting byte at a particular index.
   * 
   * @param index is the location from where the byte value has to be read.
   * @return byte value present in that location.
   */
  byte byteAt(int index);

  /**
   * for getting the array object
   * 
   * @return byte[]
   */
  byte[] getUnderlyingArray();

  /**
   * for adding a byte in the array.
   * 
   * @param ch the byte value that has to be added.
   * @return the instance after adding the byte.
   */
  DataTypes addByte(byte ch);

  /**
   * for reseting the array length.
   */
  void reset();

  /**
   * {@inheritDoc}
   * 
   * @return a clone of this instance.
   */
  DataTypes clone();
  
  /**
   * To store the value into corresponding byte array.
   * @param token
   * @return DataTypes
   */
  DataTypes addValue(String value);
  
  
  void setSplitIndex(int splitIndex);

}
