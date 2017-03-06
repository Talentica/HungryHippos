package com.talentica.hungryHippos.coordination.utility.marshaling;

import java.io.Serializable;
import java.nio.ByteBuffer;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.client.domain.MutableCharArrayStringCache;
import com.talentica.hungryHippos.client.domain.MutableDouble;
import com.talentica.hungryHippos.client.domain.MutableInteger;

/**
 * {@code DynamicMarshal} is used by the system to decode the encrypted file.
 * 
 * @author debasishc
 * @since 1/9/15.
 */
public class DynamicMarshal implements Serializable {

  private static final long serialVersionUID = -5800537222182360030L;

  private transient final MutableCharArrayStringCache MUTABLE_CHAR_ARRAY_STRING_CACHE =
      MutableCharArrayStringCache.newInstance();

  private DataDescription dataDescription;
  private static MutableInteger mutableInteger = new MutableInteger();
  private static MutableDouble mutableDouble = new MutableDouble();

  /**
   * creates a new instance of DynamicMarshal using the {@value dataDescription}
   * 
   * @param dataDescription
   */
  public DynamicMarshal(DataDescription dataDescription) {
    this.dataDescription = dataDescription;
  }

  /**
   * used to read a value from the provided {@value index} and {@value source}.
   * 
   * @param index
   * @param source
   * @return Object.
   */
  public Object readValue(int index, ByteBuffer source) {
    DataLocator locator = dataDescription.locateField(index);
    switch (locator.getDataType()) {
      case SHORT:
        return source.getShort(locator.getOffset());
      case INT:
        return readIntValue(index, source);
      case LONG:
        return source.getLong(locator.getOffset());
      case FLOAT:
        return source.getFloat(locator.getOffset());
      case DOUBLE:
        return readDoubleValue(index, source);
      case STRING:
        return readValueString(index, source);
    }
    return null;
  }

  /**
   * reads MutableCharArrayString value from the given {@value index} from ByteBuffer
   * {@value source}
   * 
   * @param index
   * @param source
   * @return {@link MutableCharArrayString}
   */
  public MutableCharArrayString readValueString(int index, ByteBuffer source) {
    DataLocator locator = dataDescription.locateField(index);
    int offset = locator.getOffset();
    int size = locator.getSize();
    MutableCharArrayString charArrayString =
        MUTABLE_CHAR_ARRAY_STRING_CACHE.getMutableStringFromCacheOfSize(size);
    for (int i = offset; i < offset + size; i++) {
      byte ch = source.get(i);
      if (ch == 0) {
        break;
      }
      charArrayString.addByte(ch);
    }
    return charArrayString;
  }

  public MutableInteger readIntValue(int index, ByteBuffer source) {
    mutableInteger.reset();
    DataLocator locator = dataDescription.locateField(index);
    int offset = locator.getOffset();
    int size = locator.getSize();
    for (int i = offset; i < offset + size; i++) {
      byte ch = source.get(i);
      mutableInteger.addByte(ch);
    }
    return mutableInteger;
  }


  /**
   * writes the {@value Object} to an {@value index} location in the {@value dest} Buffer.
   * 
   * @param index
   * @param object
   * @param dest
   */
  public void writeValue(int index, final Object object, ByteBuffer dest) {
    DataLocator locator = dataDescription.locateField(index);
    switch (locator.getDataType()) {
      case SHORT:
        dest.putShort(locator.getOffset(), Short.parseShort(object.toString()));
        break;
      case INT:
        writeInt(object, dest, locator);
        break;
      case LONG:
        dest.putLong(locator.getOffset(), Long.parseLong(object.toString()));
        break;
      case FLOAT:
        dest.putFloat(locator.getOffset(), Float.parseFloat(object.toString()));
        break;
      case DOUBLE:
        writeDouble(object, dest, locator);
        break;
      case STRING:
        byte[] strContent = ((MutableCharArrayString) object).getValue().getBytes();
        int strOffset = locator.getOffset();
        int strSize = locator.getSize();
        int j = 0;
        int i = strOffset;
        for (; i < strOffset + strSize && j < strContent.length; i++, j++) {
          dest.put(i, strContent[j]);
        }
    }
  }

  private void writeDouble(final Object object, ByteBuffer dest, DataLocator locator) {
    byte[] doubleContent = mutableDouble.addValue(Integer.parseInt(object.toString()));
    int doubleOffset = locator.getOffset();
    int doubleSize = locator.getSize();
    int p = 0;
    int q = doubleOffset;
    for (; q < doubleOffset + doubleSize && p < doubleContent.length; q++, p++) {
      dest.put(q, doubleContent[p]);
    }
  }

  private void writeInt(final Object object, ByteBuffer dest, DataLocator locator) {
    byte[] intContent = mutableInteger.addValue(Integer.parseInt(object.toString()));
    int intOffset = locator.getOffset();
    int intSize = locator.getSize();
    int k = 0;
    int l = intOffset;
    for (; l < intOffset + intSize && k < intContent.length; l++, k++) {
      dest.put(l, intContent[k]);
    }
  }

  /**
   * writes the {@link MutableCharArrayString} {@value input} to an {@value index} location in the
   * {@value dest} Buffer.
   * 
   * @param index
   * @param input
   * @param dest
   */
  public void writeValueString(int index, MutableCharArrayString input, ByteBuffer dest) {
    DataLocator locator = dataDescription.locateField(index);
    switch (locator.getDataType()) {
      case STRING:
        int offset = locator.getOffset();
        int size = locator.getSize();
        int j = 0;
        int i = offset;
        for (; i < offset + size && j < input.length(); i++, j++) {
          dest.put(i, (byte) input.charAt(j));
        }
        break;
      default:
        throw new IllegalArgumentException("Invalid data format");
    }
  }

  /**
   * writes the double {@value input} to an {@value index} location in the {@value dest} Buffer.
   * 
   * @param index
   * @param object
   * @param dest
   */
  public void writeValueDouble(int index, double object, ByteBuffer dest) {
    DataLocator locator = dataDescription.locateField(index);
    switch (locator.getDataType()) {
      case DOUBLE:
        dest.putDouble(locator.getOffset(), object);
        break;
      default:
        throw new IllegalArgumentException("Invalid data format");
    }
  }

  public MutableDouble readDoubleValue(int index, ByteBuffer dest) {
    mutableDouble.reset();
    DataLocator locator = dataDescription.locateField(index);
    int offset = locator.getOffset();
    int size = locator.getSize();
    for (int i = offset; i < offset + size; i++) {
      byte ch = dest.get(i);
      mutableDouble.addByte(ch);
    }
    return mutableDouble;
  }

}
