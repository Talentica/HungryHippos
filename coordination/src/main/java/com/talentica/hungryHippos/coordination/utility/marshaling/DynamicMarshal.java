/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.talentica.hungryHippos.coordination.utility.marshaling;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;

import com.talentica.hungryHippos.client.domain.*;

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
   * creates a new instance of DynamicMarshal using the {dataDescription}
   * 
   * @param dataDescription
   */
  public DynamicMarshal(DataDescription dataDescription) {
    this.dataDescription = dataDescription;
  }

  /**
   * used to read a value from the provided {index} and { source}.
   * 
   * @param index
   * @param source
   * @return Object.
   */
  public Object readValue(int index, ByteBuffer source) {
    DataLocator locator = dataDescription.locateField(index);
    switch (locator.getDataType()) {
      case BYTE:
        return source.get(locator.getOffset());
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
      case DATE:
        return new Date(source.getLong(locator.getOffset()));
      case TIMESTAMP:
        return new Timestamp(source.getLong(locator.getOffset()));
    }
    return null;
  }

  /**
   * reads MutableCharArrayString value from the given { index} from ByteBuffer
   * { source}
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
   * writes the { Object} to an { index} location in the { dest} Buffer.
   * 
   * @param index
   * @param object
   * @param dest
   */
  public void writeValue(int index, final Object object, ByteBuffer dest) {
    DataLocator locator = dataDescription.locateField(index);
    switch (locator.getDataType()) {
      case BYTE:
        dest.put(locator.getOffset(), ((MutableByte)object).getByteValue());
        break;
      case SHORT:
        dest.putShort(locator.getOffset(), ((MutableShort)object).getShortValue());
        break;
      case INT:
        writeIntValue(object,dest,locator);
        break;
      case LONG:
        dest.putLong(locator.getOffset(), ((MutableLong) object).getLongValue() );
        break;
      case FLOAT:
        dest.putFloat(locator.getOffset(), ((MutableFloat)object).getFloatValue());
        break;
      case DOUBLE:
        writeDoubleValue(object,dest,locator);
        break;
      case DATE:
        dest.putLong(locator.getOffset(), ((MutableDate) object).getLongValue() );
        break;
      case TIMESTAMP:
        dest.putLong(locator.getOffset(), ((MutableTimeStamp) object).getLongValue() );
        break;
      case STRING:
        writeStringValue((MutableCharArrayString) object, dest, locator);
    }
  }

  private void writeStringValue(MutableCharArrayString object, ByteBuffer dest, DataLocator locator) {
    byte[] content = object.getValue().getBytes();
    int offset = locator.getOffset();
    int size = locator.getSize();
    int j = 0;
    int i = offset;
    for (; i < offset + size && j < content.length; i++, j++) {
      dest.put(i, content[j]);
    }
  }

  /**
   * writes the {@link MutableCharArrayString} { input} to an {index} location in the
   * { dest} Buffer.
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
   * writes the double {input} to an {index} location in the { dest} Buffer.
   *
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
