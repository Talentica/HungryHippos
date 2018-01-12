/*
 * *****************************************************************************
 *   Copyright 2017 Talentica Software Pvt. Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *  *****************************************************************************
 */
package com.talentica.hungryHippos.node.joiners;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;

/**
 * {@code HHRowReader} is used to convert data from binary form to Object form.
 * 
 * @author rajkishoreh
 *
 */
public class HHRowReader {

  /** The data description. */
  private DataDescription dataDescription;

  /** The source. */
  private ByteBuffer source;

  private int initPos;

  /**
   * Creates a new instance of {@link HHRowReader}.
   *
   * @param dataDescription        The {@link DataDescription} object for reading a particular type of record
   */
  public HHRowReader(DataDescription dataDescription, ByteBuffer source, int initPos) {
    this.dataDescription = dataDescription;
    this.source = source;
    this.initPos = initPos;
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
    int offset = initPos+locator.getOffset();
    switch (locator.getDataType()) {
      case BYTE:
        return source.get(offset);
      case SHORT:
        return source.getShort(offset);
      case INT:
        return source.getInt(offset);
      case LONG:
        return source.getLong(offset);
      case FLOAT:
        return source.getFloat(offset);
      case DOUBLE:
        return source.getDouble(offset);
      case STRING:
        return readValueString(offset,locator.getSize());
      case DATE:
        return new Date(source.getLong(offset));
      case TIMESTAMP:
        return new Timestamp(source.getLong(offset));
    }
    return null;
  }


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
