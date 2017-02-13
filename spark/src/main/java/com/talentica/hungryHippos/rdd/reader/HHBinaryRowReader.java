package com.talentica.hungryHippos.rdd.reader;

import java.io.Serializable;
import java.nio.ByteBuffer;

import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;

/**
 * {@code DynamicMarshal} is used by the system to decode the encrypted file.
 * 
 * @author debasishc
 * @since 1/9/15.
 */
public class HHBinaryRowReader<T> implements Serializable,HHRowReader<byte[]> {

  private static final long serialVersionUID = -5800537222182360030L;
  private FieldTypeArrayDataDescription dataDescription;
  private ByteBuffer source = null;
  private int allocatedSize = 0;
  private byte[] b;

  /**
   * It wraps over the byte[] and create the new HeapByteBuffer if an only if byte[] array has
   * different capacity than earlier otherwise reuse the existing one.
   * 
   * @param b
   * @return instance of the current object
   */
  @Override
  public  HHBinaryRowReader<byte[]> wrap(byte[] b) {
    this.b = b;
    if (source == null) {
      allocatedSize = this.b.length;
      source = ByteBuffer.wrap(this.b);
    } else {
      if (allocatedSize != this.b.length) {
        allocatedSize = this.b.length;
        source = ByteBuffer.wrap(this.b);
      }
    }
    source.clear();
    return (HHBinaryRowReader<byte[]>) this;
  }

  public ByteBuffer getByteBuffer() {
    return this.source;
  }

  /**
   * creates a new instance of DynamicMarshal using the {@value dataDescription}
   * 
   * @param dataDescription
   */
  public HHBinaryRowReader(FieldTypeArrayDataDescription dataDescription) {
    this.dataDescription = dataDescription;
  }

  public FieldTypeArrayDataDescription getFieldDataDescription() {
    return this.dataDescription;
  }

  /**
   * used to read a value from the provided {@value index} and {@value source}.
   * 
   * @param index
   * @param source
   * @return Object.
   */
  @Override
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
