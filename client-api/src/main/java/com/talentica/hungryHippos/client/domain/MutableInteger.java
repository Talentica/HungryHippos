package com.talentica.hungryHippos.client.domain;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * {@code MutableInteger} is used for memory optimization. Because of this class system will be
 * making less objects for Integer.
 * 
 * @author sudarshans
 *
 */
public class MutableInteger implements DataTypes {

  private static final long serialVersionUID = -6085804645390531875L;
  private byte[] array;
  private int length = Integer.BYTES;

  /**
   * creates a new MutableInteger with specified length. The length specified is the limit of the
   * underlying array.
   * 
   * @param length
   */
  public MutableInteger() {
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


  private void copyCharacters(int start, int end, MutableInteger newArray) {
    for (int i = start, j = 0; i < end; i++, j++) {
      newArray.array[j] = array[i];
    }
  }

  @Override
  public String toString() {
    return new String(Arrays.copyOf(array, length));
  }


  @Override
  public void reset() {
    index = 0;
  }

  @Override
  public MutableInteger clone() {
    MutableInteger newArray = new MutableInteger();
    copyCharacters(0, length, newArray);
    newArray.length = length;
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
    if (length == that.length) {
      for (int i = 0; i < length; i++) {
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
    int len = length;
    for (int i = 0; i < len; i++) {
      h = 31 * h + val[off++];
    }
    return h;
  }

  /**
   * create a new MutableDouble from the value provided.
   * 
   * @param value
   * @return
   * @throws InvalidRowException
   */
  public static MutableInteger from(String value) throws InvalidRowException {
    MutableInteger mutableInteger = new MutableInteger();
    for (byte character : value.getBytes(StandardCharsets.UTF_8)) {
      mutableInteger.addByte(character);
    }
    return mutableInteger;
  }

  @Override
  public int compareTo(DataTypes dataType) {
    MutableInteger otherMutableIntegerByteArray = null;
    if (dataType instanceof MutableInteger) {
      otherMutableIntegerByteArray = (MutableInteger) dataType;
    } else {
      return -1;
    }
    if (equals(otherMutableIntegerByteArray)) {
      return 0;
    }
    if (length != otherMutableIntegerByteArray.length) {
      return Integer.valueOf(length).compareTo(otherMutableIntegerByteArray.length);
    }
    for (int i = 0; i < length; i++) {
      if (array[i] == otherMutableIntegerByteArray.array[i]) {
        continue;
      }
      return array[i] - otherMutableIntegerByteArray.array[i];
    }
    return 0;
  }

  public byte[] addValue(int data) {
    array[0] = (byte) ((data >> 24) & 0xff);
    array[1] = (byte) ((data >> 16) & 0xff);
    array[2] = (byte) ((data >> 8) & 0xff);
    array[3] = (byte) ((data >> 0) & 0xff);
    return array;
  }



  public int toInt() {
    if (array == null || array.length != 4)
      return 0x0;
    return (int) ((0xff & array[0]) << 24 | (0xff & array[1]) << 16 | (0xff & array[2]) << 8
        | (0xff & array[3]) << 0);
  }

  int index = 0;

  @Override
  public MutableInteger addByte(byte b) {
    array[index++] = b;
    return this;
  }

}
