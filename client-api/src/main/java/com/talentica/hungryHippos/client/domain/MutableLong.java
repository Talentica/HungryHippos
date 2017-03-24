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
 * {@code MutableLong} is used for memory optimization. Because of this class system will 
 * be making less objects for Long.
 * @author sudarshans
 *
 */
public class MutableLong implements DataTypes {

  private static final long serialVersionUID = -6085804645390531875L;
  private byte[] array;
  private int stringLength = Long.BYTES;

  /**
   * creates a new MutableLong with specified length. The length specified is the limit
   * of the underlying array.
   * 
   * @param length
   */
  public MutableLong(int length) {
    array = new byte[length];
  }
  
  @Override
  public int getLength() {
    return stringLength;
  }

  @Override
  public byte byteAt(int index) {
    return array[index];
  }

  @Override
  public byte[] getUnderlyingArray() {
    return array;
  }


  private void copyCharacters(int start, int end, MutableLong newArray) {
    for (int i = start, j = 0; i < end; i++, j++) {
      newArray.array[j] = array[i];
    }
  }

  @Override
  public String toString() {
    return new String(Arrays.copyOf(array, stringLength));
  }

  @Override
  public MutableLong addByte(byte ch) {
    array[stringLength] = ch;
    stringLength++;
    return this;
  }

  @Override
  public void reset() {
    stringLength = 0;
  }

  @Override
  public MutableLong clone() {
    MutableLong newArray = new MutableLong(stringLength);
    copyCharacters(0, stringLength, newArray);
    newArray.stringLength = stringLength;
    return newArray;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || !(o instanceof MutableLong)) {
      return false;
    }
    MutableLong that = (MutableLong) o;
    if (stringLength == that.stringLength) {
      for (int i = 0; i < stringLength; i++) {
        if (array[i] != that.array[i]) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h = 0;
    int off = 0;
    byte val[] = array;
    int len = stringLength;
    for (int i = 0; i < len; i++) {
      h = 31 * h + val[off++];
    }
    return h;
  }

  /**
   * create a new MutableLong from the value provided.
   * 
   * @param value
   * @return
   * @throws InvalidRowException
   */
  public static MutableLong from(String value) throws InvalidRowException {
    MutableLong mutableLongByteArray = new MutableLong(value.length());
    for (byte character : value.getBytes(StandardCharsets.UTF_8)) {
      mutableLongByteArray.addByte(character);
    }
    return mutableLongByteArray;
  }

  @Override
  public int compareTo(DataTypes dataType) {
    MutableLong otherMutableLongByteArray = null;
    if (dataType instanceof MutableLong) {
      otherMutableLongByteArray = (MutableLong) dataType;
    } else {
      return -1;
    }
    if (equals(otherMutableLongByteArray)) {
      return 0;
    }
    if (stringLength != otherMutableLongByteArray.stringLength) {
      return Integer.valueOf(stringLength).compareTo(otherMutableLongByteArray.stringLength);
    }
    for (int i = 0; i < stringLength; i++) {
      if (array[i] == otherMutableLongByteArray.array[i]) {
        continue;
      }
      return array[i] - otherMutableLongByteArray.array[i];
    }
    return 0;
  }

  @Override
  public DataTypes addValue(String value) {
    // TODO Auto-generated method stub
    return null;
  }



}
