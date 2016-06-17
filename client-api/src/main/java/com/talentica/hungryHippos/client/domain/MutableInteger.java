package com.talentica.hungryHippos.client.domain;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * 
 * @author sudarshans
 *
 */
public class MutableInteger implements DataTypes {

  private static final long serialVersionUID = -6085804645390531875L;
  private byte[] array;
  private int stringLength;

  public MutableInteger(int length) {
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

  public byte[] getUnderlyingArray() {
    return array;
  }


  private void copyCharacters(int start, int end, MutableInteger newArray) {
    for (int i = start, j = 0; i < end; i++, j++) {
      newArray.array[j] = array[i];
    }
  }

  @Override
  public String toString() {
    return new String(Arrays.copyOf(array, stringLength));
  }

  public MutableInteger addByte(byte ch) {
    array[stringLength] = ch;
    stringLength++;
    return this;
  }

  public void reset() {
    stringLength = 0;
  }

  @Override
  public MutableInteger clone() {
    MutableInteger newArray = new MutableInteger(stringLength);
    copyCharacters(0, stringLength, newArray);
    newArray.stringLength = stringLength;
    return newArray;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || !(o instanceof MutableInteger)) {
      return false;
    }
    MutableInteger that = (MutableInteger) o;
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
    MutableInteger otherMutableIntegerByteArray = null;
    if(dataType instanceof MutableInteger){
      otherMutableIntegerByteArray = (MutableInteger)dataType;
    }else {
      return -1;
    }
    if (equals(otherMutableIntegerByteArray)) {
      return 0;
    }
    if (stringLength != otherMutableIntegerByteArray.stringLength) {
      return Integer.valueOf(stringLength).compareTo(otherMutableIntegerByteArray.stringLength);
    }
    for (int i = 0; i < stringLength; i++) {
      if (array[i] == otherMutableIntegerByteArray.array[i]) {
        continue;
      }
      return array[i] - otherMutableIntegerByteArray.array[i];
    }
    return 0;
  }


}
