package com.talentica.hungryHippos.client.domain;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * 
 * @author sudarshans
 *
 */
public class MutableShortByteArray implements Comparable<MutableShortByteArray>, DataType {

  private static final long serialVersionUID = -6085804645390531875L;
  private byte[] array;
  private int stringLength;

  public MutableShortByteArray(int length) {
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


  private void copyCharacters(int start, int end, MutableShortByteArray newArray) {
    for (int i = start, j = 0; i < end; i++, j++) {
      newArray.array[j] = array[i];
    }
  }

  @Override
  public String toString() {
    return new String(Arrays.copyOf(array, stringLength));
  }

  public MutableShortByteArray addByte(byte ch) {
    array[stringLength] = ch;
    stringLength++;
    return this;
  }

  public void reset() {
    stringLength = 0;
  }

  @Override
  public MutableShortByteArray clone() {
    MutableShortByteArray newArray = new MutableShortByteArray(stringLength);
    copyCharacters(0, stringLength, newArray);
    newArray.stringLength = stringLength;
    return newArray;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || !(o instanceof MutableShortByteArray)) {
      return false;
    }
    MutableShortByteArray that = (MutableShortByteArray) o;
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

  public static MutableLongByteArray from(String value) throws InvalidRowException {
    MutableLongByteArray mutableLongByteArray = new MutableLongByteArray(value.length());
    for (byte character : value.getBytes(StandardCharsets.UTF_8)) {
      mutableLongByteArray.addByte(character);
    }
    return mutableLongByteArray;
  }

  @Override
  public int compareTo(MutableShortByteArray otherMutableShortByteArray) {
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
