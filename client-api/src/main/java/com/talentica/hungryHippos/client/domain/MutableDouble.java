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

import java.util.Arrays;

/**
 * {@code MutableDouble} is used for memory optimization. Because of this class system will be
 * making less objects for Double.
 * 
 * @author sudarshans
 *
 */
public class MutableDouble implements DataTypes {

  private static final long serialVersionUID = -6085804645390531875L;
  private byte[] array;
  private int length = Double.BYTES;

  /**
   * creates a new MutableDouble.
   * 
   */
  public MutableDouble() {
    array = new byte[length];
  }

  @Override
  public int getLength() {
    return length;
  }

  @Override
  public byte byteAt(int index) {
    return array[index];
  }

  @Override
  public byte[] getUnderlyingArray() {
    return array;
  }


  @Override
  public String toString() {
    return toDouble() + "";
  }

  @Override
  public void reset() {
    index = 0;
  }

  @Override
  public MutableDouble clone() {
    MutableDouble newArray = new MutableDouble();
    System.arraycopy(array, 0, newArray.array, 0, length);
    return newArray;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || !(o instanceof MutableInteger)) {
      return false;
    }
    MutableDouble that = (MutableDouble) o;
      for (int i = 0; i < length; i++) {
        if (array[i] != that.array[i]) {
          return false;
        }
      }
      return true;
  }

  @Override
  public int hashCode() {
    int h = 0;
    for (int i = 0; i < length; i++) {
      h = 31 * h + array[i];
    }
    return h;
  }

  @Override
  public int compareTo(DataTypes dataType) {
    MutableDouble otherMutableDoubleByteArray = null;
    if (dataType instanceof MutableDouble) {
      otherMutableDoubleByteArray = (MutableDouble) dataType;
    } else {
      return -1;
    }
    if (equals(dataType)) {
      return 0;
    }
    if (length != otherMutableDoubleByteArray.length) {
      return Integer.valueOf(length).compareTo(otherMutableDoubleByteArray.length);
    }
    for (int i = 0; i < length; i++) {
      if (array[i] == otherMutableDoubleByteArray.array[i]) {
        continue;
      }
      return array[i] - otherMutableDoubleByteArray.array[i];
    }
    return 0;
  }

  @Override
  public MutableDouble addValue(String value) {
    Long lng = Double.doubleToLongBits(parseDouble(value));
    for (int i = 0; i < 8; i++) {
      array[i] = (byte) ((lng >> ((7 - i) * 8)) & 0xff);
    }
    return this;
  }

  public double toDouble() {
    if (array == null || array.length != Double.BYTES) {
      return 0x0;
    }
    return Double.longBitsToDouble(toLong());
  }


  public MutableDouble addByte(byte b, int index) {
    array[index++] = b;
    return this;
  }


  public long toLong() {
    if (array == null || array.length != Double.BYTES) {
      return 0x0;
    }
    return (long) ((long) (0xff & array[0]) << 56 | (long) (0xff & array[1]) << 48
        | (long) (0xff & array[2]) << 40 | (long) (0xff & array[3]) << 32
        | (long) (0xff & array[4]) << 24 | (long) (0xff & array[5]) << 16
        | (long) (0xff & array[6]) << 8 | (long) (0xff & array[7]) << 0);
  }

  private int index = 0;

  @Override
  public MutableDouble addByte(byte ch) {
    array[index++] = ch;
    return this;
  }
  
  public double parseDouble(String value){
    return Double.parseDouble(value);
  }

}