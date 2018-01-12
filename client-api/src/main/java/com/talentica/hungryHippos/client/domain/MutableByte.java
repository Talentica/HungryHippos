/*
 * *****************************************************************************
 *   Copyright 2017 Talentica Software Pvt. Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *  *****************************************************************************
 */
package com.talentica.hungryHippos.client.domain;

/**
 * Created by rajkishoreh on 12/10/17.
 */
public class MutableByte implements DataTypes {

  private static final long serialVersionUID = -6085804645390531875L;
  private byte byteValue;


  /**
   * creates a new {@link MutableByte} with specified length. The length specified is the limit of the
   * underlying array.
   *
   */
  public MutableByte() {
  }

  @Override
  public int getLength() {
    return Byte.BYTES;
  }


  @Override
  public String toString() {
    return byteValue +"";
  }


  @Override
  public void reset() {

  }

  @Override
  public MutableByte clone() {
    MutableByte mutableByte = new MutableByte();
    mutableByte.byteValue = this.byteValue;
    return mutableByte;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || !(o instanceof MutableByte)) {
      return false;
    }
    MutableByte that = (MutableByte) o;
    return this.byteValue ==that.byteValue;
  }

  @Override
  public int hashCode() {
    return Byte.hashCode(this.byteValue);
  }

  @Override
  public int compareTo(DataTypes dataType) {
    MutableByte otherMutableByte = null;
    if (dataType instanceof MutableByte) {
      otherMutableByte = (MutableByte) dataType;
    } else {
      return -1;
    }
    return Byte.compare(this.byteValue,otherMutableByte.byteValue);
  }

  @Override
  public DataTypes addValue(String value) {
    byteValue = Byte.valueOf(value);
    return this;
  }

  public byte getByteValue() {
    return byteValue;
  }
}
