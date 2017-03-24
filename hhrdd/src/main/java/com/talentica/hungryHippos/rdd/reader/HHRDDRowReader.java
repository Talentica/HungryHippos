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
package com.talentica.hungryHippos.rdd.reader;

import java.nio.ByteBuffer;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;

/**
 * {@code HHRDDRowReader} is used to convert data from binary form to Object form.
 * 
 * @author sudarshans
 *
 */
public class HHRDDRowReader {

  private DataDescription dataDescription;

  private ByteBuffer source = null;

  /**
   * Sets the buffer for the object
   * @param source
   *        A byte buffer object
     */
  public void setByteBuffer(ByteBuffer source) {
    this.source = source;
  }

  public ByteBuffer getByteBuffer() {
    return this.source;
  }

  /**
   * Creates a new instance of {@link HHRDDRowReader}
   * 
   * @param dataDescription
   *        The {@link DataDescription} object for reading a particular type of record
   */
  public HHRDDRowReader(DataDescription dataDescription) {
    this.dataDescription = dataDescription;
  }

  public DataDescription getFieldDataDescription() {
    return this.dataDescription;
  }

  /**
   * Reads value from its buffer.
   * 
   * @param index
   *        The index of the column
   * @return Object.
   *        An instance of either a Byte or a Character or an Integer or a Long or
   *        a Float or a Double or a String depending on the {@link DataDescription}
   */
  public Object readAtColumn(int index) {
    DataLocator locator = dataDescription.locateField(index);
    switch (locator.getDataType()) {
      case BYTE:
        return source.get(locator.getOffset());
      case CHAR:
        return source.getChar(locator.getOffset());
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
        return readValueString(index);
    }
    return null;
  }

  /**
   * Reads MutableCharArrayString value from {@link ByteBuffer}
   * 
   * @param index
   * @return {@link MutableCharArrayString}
   */
  private MutableCharArrayString readValueString(int index) {
    DataLocator locator = dataDescription.locateField(index);
    int offset = locator.getOffset();
    int size = locator.getSize();
    MutableCharArrayString charArrayString = new MutableCharArrayString(size);
    for (int i = offset; i < offset + size; i++) {
      byte ch = source.get(i);
      if (ch == 0) {
        break;
      }
      charArrayString.addByte(ch);
    }
    return charArrayString;
  }

}
