package com.talentica.hungryHippos.client.domain;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * 
 * @author sudarshans
 *
 */
public class MutableLong implements DataTypes {

  private static final long serialVersionUID = -6085804645390531875L;
  private byte[] array;
  private int stringLength;

  public MutableLong(int length) {
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


  private void copyCharacters(int start, int end, MutableLong newArray) {
    for (int i = start, j = 0; i < end; i++, j++) {
      newArray.array[j] = array[i];
    }
  }

  @Override
  public String toString() {
    return new String(Arrays.copyOf(array, stringLength));
  }

  public MutableLong addByte(byte ch) {
    array[stringLength] = ch;
    stringLength++;
    return this;
  }

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

  public static MutableInteger from(String value) throws InvalidRowException {
    MutableInteger mutableLongByteArray = new MutableInteger(value.length());
    for (byte character : value.getBytes(StandardCharsets.UTF_8)) {
      mutableLongByteArray.addByte(character);
    }
    return mutableLongByteArray;
  }

  @Override
  public int compareTo(DataTypes dataType) {
    MutableLong otherMutableLongByteArray = null;
    if(dataType instanceof MutableLong ){
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



}
