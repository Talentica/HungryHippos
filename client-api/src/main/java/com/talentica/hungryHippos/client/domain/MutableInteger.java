package com.talentica.hungryHippos.client.domain;

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
   * creates a new MutableInteger.
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
    MutableInteger that = (MutableInteger) o;
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

  public MutableInteger addValue(StringBuilder data) {
    int value = 0;
    for (int index = 0; index < data.length(); index++) {
      value = Character.digit(data.charAt(index), 10) + 10 * value;
    }
    array[0] = (byte) ((value >> 24) & 0xff);
    array[1] = (byte) ((value >> 16) & 0xff);
    array[2] = (byte) ((value >> 8) & 0xff);
    array[3] = (byte) ((value >> 0) & 0xff);
    return this;
  }



  public int toInt() {
    if (array == null || array.length != 4)
      return 0x0;
    return (int) ((0xff & array[0]) << 24 | (0xff & array[1]) << 16 | (0xff & array[2]) << 8
        | (0xff & array[3]) << 0);
  }

  private int index = 0;

  @Override
  public DataTypes addByte(byte ch) {
    array[index++] = ch;
    return this;
  }

}
