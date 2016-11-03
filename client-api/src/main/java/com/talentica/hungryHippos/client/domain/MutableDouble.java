package com.talentica.hungryHippos.client.domain;

import java.nio.charset.StandardCharsets;
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
  private int stringLength;

  /**
   * creates a new MutableDouble with specified length. The length specified is the limit of the
   * underlying array.
   * 
   * @param length is the size of backing array.
   */
  public MutableDouble(int length) {
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


  private void copyCharacters(int start, int end, MutableDouble newArray) {
    for (int i = start, j = 0; i < end; i++, j++) {
      newArray.array[j] = array[i];
    }
  }

  @Override
  public String toString() {
    return new String(Arrays.copyOf(array, stringLength));
  }

  @Override
  public MutableDouble addByte(byte ch) {
    array[stringLength] = ch;
    stringLength++;
    return this;
  }

  @Override
  public void reset() {
    stringLength = 0;
  }

  @Override
  public MutableDouble clone() {
    MutableDouble newArray = new MutableDouble(stringLength);
    copyCharacters(0, stringLength, newArray);
    newArray.stringLength = stringLength;
    return newArray;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || !(o instanceof MutableDouble)) {
      return false;
    }
    MutableDouble that = (MutableDouble) o;
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
   * create a new MutableDouble from the value provided.
   * 
   * @param value from which you want to create a MutableDouble.R
   * @return a new MutableDouble created using the value provided.
   * @throws InvalidRowException if the value can't be converted to MutableDouble.
   */
  public static MutableDouble from(String value) throws InvalidRowException {
    MutableDouble mutableDoubleByteArray = new MutableDouble(value.length());
    for (byte character : value.getBytes(StandardCharsets.UTF_8)) {
      mutableDoubleByteArray.addByte(character);
    }
    return mutableDoubleByteArray;
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
    if (stringLength != otherMutableDoubleByteArray.stringLength) {
      return Integer.valueOf(stringLength).compareTo(otherMutableDoubleByteArray.stringLength);
    }
    for (int i = 0; i < stringLength; i++) {
      if (array[i] == otherMutableDoubleByteArray.array[i]) {
        continue;
      }
      return array[i] - otherMutableDoubleByteArray.array[i];
    }
    return 0;
  }

}
