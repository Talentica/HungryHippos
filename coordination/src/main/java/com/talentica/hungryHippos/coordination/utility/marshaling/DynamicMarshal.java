package com.talentica.hungryHippos.coordination.utility.marshaling;

import java.io.Serializable;
import java.nio.ByteBuffer;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.client.domain.MutableCharArrayStringCache;

/**
 * {@code DynamicMarshal} is used by the system to decode the encrypted file.
 * 
 * @author debasishc
 * @since 1/9/15.
 */
public class DynamicMarshal {

  private final MutableCharArrayStringCache MUTABLE_CHAR_ARRAY_STRING_CACHE =
      MutableCharArrayStringCache.newInstance();

  private DataDescription dataDescription;

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
        return source.getInt(locator.getOffset());
      case LONG:
        return source.getLong(locator.getOffset());
      case FLOAT:
        return source.getFloat(locator.getOffset());
      case DOUBLE:
        return source.getDouble(locator.getOffset());
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
        dest.putInt(locator.getOffset(), Integer.parseInt(object.toString()));
        break;
      case LONG:
        dest.putLong(locator.getOffset(), Long.parseLong(object.toString()));
        break;
      case FLOAT:
        dest.putFloat(locator.getOffset(), Float.parseFloat(object.toString()));
        break;
      case DOUBLE:
        dest.putDouble(locator.getOffset(), Double.parseDouble(object.toString()));
        break;
      case STRING:
        byte[] content = ((MutableCharArrayString)object).getValue().getBytes();
        int offset = locator.getOffset();
        int size = locator.getSize();
        int j = 0;
        int i = offset;
        for (; i < offset + size && j < content.length; i++, j++) {
          dest.put(i, content[j]);
        }
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

}
