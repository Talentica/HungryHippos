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
import java.util.Comparator;

/**
 * {@code MutableLong} is used for memory optimization. Because of this class system will 
 * be making less objects for Long.
 * @author sudarshans
 *
 */
public class MutableLong implements DataTypes {

  private static final long serialVersionUID = -6085804645390531875L;
  private long longValue;

  /**
   * creates a new MutableLong with specified length. The length specified is the limit
   * of the underlying array.
   */
  public MutableLong() {
  }
  
  @Override
  public int getLength() {
    return Long.BYTES;
  }

  @Override
  public String toString() {
    return longValue+"";
  }

  @Override
  public void reset() {

  }

  @Override
  public MutableLong clone() {
    MutableLong mutableLong = new MutableLong();
    mutableLong.longValue = this.longValue;
    return mutableLong;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || !(o instanceof MutableLong)) {
      return false;
    }
    MutableLong that = (MutableLong) o;
    return this.longValue == that.longValue;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(this.longValue);
  }

  @Override
  public int compareTo(DataTypes dataType) {
    MutableLong otherMutableLongByteArray = null;
    if (dataType instanceof MutableLong) {
      otherMutableLongByteArray = (MutableLong) dataType;
    } else {
      return -1;
    }
    return Long.compare(this.longValue,otherMutableLongByteArray.longValue);
  }

  @Override
  public DataTypes addValue(String value) {
    longValue = Long.valueOf(value);
    return this;
  }

  public long getLongValue() {
    return longValue;
  }
}
