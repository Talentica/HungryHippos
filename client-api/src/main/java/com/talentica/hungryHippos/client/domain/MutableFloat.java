/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
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
 * {@code MutableFloat} is used for memory optimization. Because of this class system will be making
 * less objects for Float.
 * 
 * @author sudarshans
 *
 */
public class MutableFloat implements DataTypes {

  private static final long serialVersionUID = -6085804645390531875L;
  private byte[] array;
  private int stringLength;


  /**
   * creates a new MutableFloat with specified length. The length specified is the limit of the
   * underlying array.
   * 
   * @param length
   */
  public MutableFloat(int length) {
    array = new byte[length];
    stringLength = 0;
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


  private void copyCharacters(int start, int end, MutableFloat newArray) {
    for (int i = start, j = 0; i < end; i++, j++) {
      newArray.array[j] = array[i];
    }
  }

  @Override
  public String toString() {
    return new String(Arrays.copyOf(array, stringLength));
  }

  @Override
  public MutableFloat addByte(byte ch) {
    array[stringLength] = ch;
    stringLength++;
    return this;
  }

  @Override
  public void reset() {
    stringLength = 0;
  }

  @Override
  public MutableFloat clone() {
    MutableFloat newArray = new MutableFloat(stringLength);
    copyCharacters(0, stringLength, newArray);
    newArray.stringLength = stringLength;
    return newArray;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || !(o instanceof MutableFloat)) {
      return false;
    }
    MutableFloat that = (MutableFloat) o;
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
   * create a new MutableFloat from the value provided.
   * 
   * @param value from which you want to create a MutableDouble.
   * @return
   * @throws InvalidRowException
   */
  public static MutableFloat from(String value) throws InvalidRowException {
    MutableFloat mutableFloatByteArray = new MutableFloat(value.length());
    for (byte character : value.getBytes(StandardCharsets.UTF_8)) {
      mutableFloatByteArray.addByte(character);
    }
    return mutableFloatByteArray;
  }

  @Override
  public int compareTo(DataTypes dataType) {
    MutableFloat otherMutableShortByteArray = null;
    if (dataType instanceof MutableFloat) {
      otherMutableShortByteArray = (MutableFloat) dataType;
    } else {
      return -1;
    }
    if (equals(otherMutableShortByteArray)) {
      return 0;
    }
    if (stringLength != otherMutableShortByteArray.stringLength) {
      return Integer.valueOf(stringLength).compareTo(otherMutableShortByteArray.stringLength);
    }
    for (int i = 0; i < stringLength; i++) {
      if (array[i] == otherMutableShortByteArray.array[i]) {
        continue;
      }
      return array[i] - otherMutableShortByteArray.array[i];
    }
    return 0;
  }

  @Override
  public DataTypes addValue(String value) {
    // TODO Auto-generated method stub
    return null;
  }


}
