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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * {@code MutableShort} is used for memory optimization. Because of this class system will be making
 * less objects for Short.
 * 
 * @author sudarshans
 *
 */
public class MutableShort implements DataTypes {

  private static final long serialVersionUID = -6085804645390531875L;
  private short shortValue;

  /**
   * creates a new MutableShort with specified length. The length specified is the limit of the
   * underlying array.
   */
  public MutableShort() {
  }

  @Override
  public int getLength() {
    return Short.BYTES;
  }


  @Override
  public String toString() {
    return shortValue+"";
  }


  @Override
  public void reset() {

  }

  @Override
  public MutableShort clone() {
    MutableShort mutableShort = new MutableShort();
    mutableShort.shortValue = this.shortValue;
    return mutableShort;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || !(o instanceof MutableShort)) {
      return false;
    }
    MutableShort that = (MutableShort) o;
    return this.shortValue==that.shortValue;
  }

  @Override
  public int hashCode() {
    return Short.hashCode(shortValue);
  }


  @Override
  public int compareTo(DataTypes dataType) {
    MutableShort otherMutableShort = null;
    if (dataType instanceof MutableShort) {
      otherMutableShort = (MutableShort) dataType;
    }
    return Short.compare(this.shortValue,otherMutableShort.shortValue);
  }

  @Override
  public DataTypes addValue(String value) {
    this.shortValue = Short.valueOf(value);
    return this;
  }

  public short getShortValue() {
    return shortValue;
  }
}
