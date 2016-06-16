package com.talentica.hungryHippos.client.domain;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * 
 * @author sudarshans
 *
 */
public class MutableFloatByteArray implements Comparable<MutableFloatByteArray>, DataType {

  private static final long serialVersionUID = -6085804645390531875L;
  private byte[] array;
  private int stringLength;

  public MutableFloatByteArray(int length) {
    array = new byte[length];
    stringLength = 0;
  }

  public int getLength() {
    return stringLength;
  }

  public byte byteAt(int index) {
    return array[index];
  }

  public byte[] getUnderlyingArray() {
    return array;
  }


  private void copyCharacters(int start, int end, MutableFloatByteArray newArray) {
    for (int i = start, j = 0; i < end; i++, j++) {
      newArray.array[j] = array[i];
    }
  }

  @Override
  public String toString() {
    return new String(Arrays.copyOf(array, stringLength));
  }

  public MutableFloatByteArray addByte(byte ch) {
    array[stringLength] = ch;
    stringLength++;
    return this;
  }

  public void reset() {
    stringLength = 0;
  }

  @Override
  public MutableFloatByteArray clone() {
    MutableFloatByteArray newArray = new MutableFloatByteArray(stringLength);
    copyCharacters(0, stringLength, newArray);
    newArray.stringLength = stringLength;
    return newArray;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || !(o instanceof MutableFloatByteArray)) {
      return false;
    }
    MutableFloatByteArray that = (MutableFloatByteArray) o;
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

  public static MutableFloatByteArray from(String value) throws InvalidRowException {
    MutableFloatByteArray mutableFloatByteArray = new MutableFloatByteArray(value.length());
    for (byte character : value.getBytes(StandardCharsets.UTF_8)) {
      mutableFloatByteArray.addByte(character);
    }
    return mutableFloatByteArray;
  }

  @Override
  public int compareTo(MutableFloatByteArray otherMutableShortByteArray) {
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
  public DataType addCharacter(char ch) {
    throw new UnsupportedOperationException("This operation is not supported for this class");
  }

}
