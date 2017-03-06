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


  private void copyCharacters(int start, int end, MutableDouble newArray) {
    for (int i = start, j = 0; i < end; i++, j++) {
      newArray.array[j] = array[i];
    }
  }

  @Override
  public String toString() {
    return new String(Arrays.copyOf(array, length));
  }

  @Override
  public void reset() {}

  @Override
  public MutableDouble clone() {
    MutableDouble newArray = new MutableDouble();
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
    MutableDouble that = (MutableDouble) o;
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
   * @param value from which you want to create a MutableDouble.R
   * @return a new MutableDouble created using the value provided.
   * @throws InvalidRowException if the value can't be converted to MutableDouble.
   */
  public static MutableDouble from(String value) throws InvalidRowException {
    MutableDouble mutableDoubleByteArray = new MutableDouble();
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

  public byte[] addValue(double data) {
    Long lng = Double.doubleToLongBits(data);
    for (int i = 0; i < 8; i++) {
      array[i] = (byte) ((lng >> ((7 - i) * 8)) & 0xff);
    }
    return array;
  }

  public double toDouble() {
    if (array == null || array.length != 8)
      return 0x0;
    return Double.longBitsToDouble(toLong(array));
  }


  public MutableDouble addByte(byte b, int index) {
    array[index++] = b;
    return this;
  }


  public byte[] toByte(double data) {
    return toByte(Double.doubleToRawLongBits(data));
  }

  public long toLong(byte[] data) {
    if (data == null || data.length != 8)
      return 0x0;
    return (long) ((long) (0xff & data[0]) << 56 | (long) (0xff & data[1]) << 48
        | (long) (0xff & data[2]) << 40 | (long) (0xff & data[3]) << 32
        | (long) (0xff & data[4]) << 24 | (long) (0xff & data[5]) << 16
        | (long) (0xff & data[6]) << 8 | (long) (0xff & data[7]) << 0);
  }

  @Override
  public MutableDouble addByte(byte ch) {
    // TODO Auto-generated method stub
    return null;
  }
}
