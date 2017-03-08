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
  private MutableInteger mutableInteger;


  /**
   * creates a new instance of DynamicMarshal using the {@value dataDescription}
   * 
   * @param dataDescription
   */
  public DynamicMarshal(DataDescription dataDescription) {
    this.dataDescription = dataDescription;
    this.mutableInteger = new MutableInteger();
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

  public MutableInteger readIntValue(int index, ByteBuffer source) {
    DataLocator locator = dataDescription.locateField(index);
    int offset = locator.getOffset();
    int size = locator.getSize();
    mutableInteger.reset();
    for (int i = offset; i < offset + size; i++) {
      byte ch = source.get(i);
      mutableInteger.addByte(ch);
    }
    return mutableInteger;
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
        writeIntValue(object, dest, locator);
        break;
      case LONG:
        dest.putLong(locator.getOffset(), Long.parseLong(object.toString()));
        break;
      case FLOAT:
        dest.putFloat(locator.getOffset(), Float.parseFloat(object.toString()));
        break;
      case DOUBLE:
        writeDoubleValue(object, dest, locator);
        break;
      case STRING:
        byte[] content = ((MutableCharArrayString) object).getValue().getBytes();
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

  private void writeIntValue(final Object object, ByteBuffer dest, DataLocator locator) {
    byte[] intContent = ((MutableInteger) object).getUnderlyingArray();
    int intOffset = locator.getOffset();
    int intSize = locator.getSize();
    int k = 0;
    int l = intOffset;
    for (; l < intOffset + intSize && k < intContent.length; l++, k++) {
      dest.put(l, intContent[k]);
    }
  }

  /**
   * writes the double {@value input} to an {@value index} location in the {@value dest} Buffer.
   * 
   * @param index
   * @param object
   * @param dest
   */
  public void writeDoubleValue(final Object object, ByteBuffer dest, DataLocator locator) {
    byte[] doubleContent = ((MutableDouble) object).getUnderlyingArray();
    int doubleOffset = locator.getOffset();
    int doubleSize = locator.getSize();
    int k = 0;
    int l = doubleOffset;
    for (; l < doubleOffset + doubleSize && k < doubleContent.length; l++, k++) {
      dest.put(l, doubleContent[k]);
    }
  }

}
