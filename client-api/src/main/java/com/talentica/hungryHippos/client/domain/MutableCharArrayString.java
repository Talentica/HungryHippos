package com.talentica.hungryHippos.client.domain;

import java.util.Arrays;

import com.talentica.hungryHippos.coordination.annotations.ZkTransient;

/**
 * Created by debasishc on 29/9/15. updated by Sudarshan
 */
public class MutableCharArrayString implements CharSequence, DataTypes {

  @ZkTransient
  private static final MutableCharArrayStringCache MUTABLE_CHAR_ARRAY_STRING_CACHE =
      MutableCharArrayStringCache.newInstance();

  @ZkTransient
  private static final long serialVersionUID = -6085804645372631875L;
  private char[] array;
  private int stringLength;

  public MutableCharArrayString(){
    
  }
  public MutableCharArrayString(int length) {
    array = new char[length];
    stringLength = 0;
  }

  @Override
  public int length() {
    return stringLength;
  }

  @Override
  public int getLength() {
    return length();
  }

  @Override
  public char charAt(int index) {
    return (char) array[index];
  }

  public byte[] getUnderlyingArray() {
    byte[] byteArray = new byte[stringLength];
    for (int i = 0; i < stringLength; i++) {
      byteArray[i] = (byte) array[i];
    }
    return byteArray;
  }

  public char[] getUnderlyingCharArray() {
    return array;
  }

  @Override
  public MutableCharArrayString subSequence(int start, int end) {
    MutableCharArrayString newArray =
        MUTABLE_CHAR_ARRAY_STRING_CACHE.getMutableStringFromCacheOfSize(end - start);
    copyCharacters(start, end, newArray);
    newArray.stringLength = end - start;
    return newArray;
  }

  private void copyCharacters(int start, int end, MutableCharArrayString newArray) {
    for (int i = start, j = 0; i < end; i++, j++) {
      newArray.array[j] = array[i];
    }
  }

  @Override
  public String toString() {
    return new String(Arrays.copyOf(array, stringLength));
  }



  public void reset() {
    stringLength = 0;
  }

  @Override
  public MutableCharArrayString clone() {
    MutableCharArrayString newArray = new MutableCharArrayString(stringLength);
    copyCharacters(0, stringLength, newArray);
    newArray.stringLength = stringLength;
    return newArray;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || !(o instanceof MutableCharArrayString)) {
      return false;
    }
    MutableCharArrayString that = (MutableCharArrayString) o;
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
    char val[] = array;
    int len = stringLength;
    for (int i = 0; i < len; i++) {
      h = 31 * h + val[off++];
    }
    return h;
  }

  @Override
  public int compareTo(DataTypes dataType) {
    MutableCharArrayString otherMutableCharArrayString = null;
    if (dataType instanceof MutableCharArrayString) {
      otherMutableCharArrayString = (MutableCharArrayString) dataType;
    } else {
      return -1;
    }
    if (equals(otherMutableCharArrayString)) {
      return 0;
    }
    if (stringLength != otherMutableCharArrayString.stringLength) {
      return Integer.valueOf(stringLength).compareTo(otherMutableCharArrayString.stringLength);
    }
    for (int i = 0; i < stringLength; i++) {
      if (array[i] == otherMutableCharArrayString.array[i]) {
        continue;
      }
      return array[i] - otherMutableCharArrayString.array[i];
    }
    return 0;
  }


  @Override
  public byte byteAt(int index) {
    return (byte) array[index];
  }


  @Override
  public DataTypes addByte(byte ch) {
    array[stringLength] = (char) ch;
    stringLength++;
    return this;
  }

  public DataTypes addCharacter(char ch) {
    array[stringLength] = ch;
    stringLength++;
    return this;
  }

}
