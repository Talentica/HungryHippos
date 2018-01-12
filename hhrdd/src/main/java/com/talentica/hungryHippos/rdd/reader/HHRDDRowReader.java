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
import java.sql.Date;
import java.sql.Timestamp;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.DateTimeUtils$;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * {@code HHRDDRowReader} is used to convert data from binary form to Object form.
 * 
 * @author sudarshans
 *
 */
public class HHRDDRowReader {

  /** The data description. */
  private DataDescription dataDescription;

  /** The source. */
  private ByteBuffer source;

  /**
   * Gets the byte buffer.
   *
   * @return the byte buffer
   */
  public ByteBuffer getByteBuffer() {
    return this.source;
  }

  /**
   * Creates a new instance of {@link HHRDDRowReader}.
   *
   * @param dataDescription        The {@link DataDescription} object for reading a particular type of record
   */
  public HHRDDRowReader(DataDescription dataDescription, ByteBuffer source) {
    this.dataDescription = dataDescription;
    this.source = source;
  }

  /**
   * Gets the field data description.
   *
   * @return the field data description
   */
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
        return readValueString(locator.getOffset(),locator.getSize());
      case DATE:
        return new Date(source.getLong(locator.getOffset()));
      case TIMESTAMP:
        return new Timestamp(source.getLong(locator.getOffset()));
    }
    return null;
  }



  /**
   * Reads String value from {@link ByteBuffer}.
   *
   * @param offset
   * @param size
   * @return {@link String}
   */
  private String readValueString(int offset,int size) {
    int count = 0;
    char[] charArr = new char[size];
    int end = offset+size;
    for (int i = offset; i < end; i++, count++) {
      charArr[count] = (char) source.get(i);
      if (charArr[count] == 0) {
        break;
      }
    }
    return new String(charArr,0,count);
  }

}
