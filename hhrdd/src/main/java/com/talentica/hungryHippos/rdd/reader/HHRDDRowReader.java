package com.talentica.hungryHippos.rdd.reader;

import java.nio.ByteBuffer;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;

/**
 * {@code DynamicMarshal} is used by the system to decode the encrypted file.
 * 
 * @author debasishc
 * @since 1/9/15.
 */
public class HHRDDRowReader {

  private DataDescription dataDescription;

  private ByteBuffer source = null;

  public void setByteBuffer(ByteBuffer source) {
    this.source = source;
  }

  public ByteBuffer getByteBuffer() {
    return this.source;
  }

  /**
   * creates a new instance of DynamicMarshal using the {@value dataDescription}
   * 
   * @param dataDescription
   */
  public HHRDDRowReader(DataDescription dataDescription) {
    this.dataDescription = dataDescription;
  }

  public DataDescription getFieldDataDescription() {
    return this.dataDescription;
  }

  /**
   * used to read a value from the provided {@value index} and {@value source}.
   * 
   * @param index
   * @param source
   * @return Object.
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
   * reads MutableCharArrayString value from the given {@value index} from ByteBuffer
   * {@value source}
   * 
   * @param index
   * @param source
   * @return {@link MutableCharArrayString}
   */
  public MutableCharArrayString readValueString(int index) {
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
