package com.talentica.hungryHippos.coordination.utility.marshaling;

import java.io.Serializable;
import java.nio.ByteBuffer;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.client.domain.MutableCharArrayStringCache;

/**
 * Created by debasishc on 1/9/15.
 */
public class DynamicMarshal implements Serializable{

  private static final long serialVersionUID = -5800537222182360030L;

  private final MutableCharArrayStringCache MUTABLE_CHAR_ARRAY_STRING_CACHE =
      MutableCharArrayStringCache.newInstance();

  private DataDescription dataDescription;
  private byte[] chunk;
  private int[] dimensions;
  public DynamicMarshal(DataDescription dataDescription) {
    this.dataDescription = dataDescription;
  }
  
  public void setChunk(byte[] chunk){
    this.chunk = chunk;
  }
  
  public void setDimensions(int[] dimensions){
    this.dimensions = dimensions;
  }
  private int columnPos1 = 0;
  private int columnPos2 = 0;
  public int compare(int row1Index,int row2Index){
    int res = 0;
    int rowSize = dataDescription.getSize();
    for (int dim = 0; dim < dimensions.length; dim++) {
      columnPos1 = 0;
      columnPos2 = 0;
      DataLocator locator = dataDescription.locateField(dim);
      columnPos1 = row1Index * rowSize + locator.getOffset();
      columnPos2 = row2Index * rowSize + locator.getOffset();
      for(int pointer = 0 ; pointer < locator.getSize() ; pointer++ ){
        if(chunk[columnPos1] != chunk[columnPos2]) {
          return chunk[columnPos1] - chunk[columnPos2];
        }
        columnPos1++;
        columnPos2++;
      }
    }
    return res;
  }
  
  public Object readValue(int index, ByteBuffer source) {
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
        return readValueString(index, source);
    }
    return null;
  }

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

  public void writeValue(int index, Object object, ByteBuffer dest) {
    DataLocator locator = dataDescription.locateField(index);
    switch (locator.getDataType()) {
      case BYTE:
        dest.put(locator.getOffset(), Byte.parseByte(object.toString()));
        break;
      case CHAR:
        dest.putChar(locator.getOffset(), (Character) object);
        break;
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
        byte[] content = object.toString().getBytes();
        int offset = locator.getOffset();
        int size = locator.getSize();
        int j = 0;
        int i = offset;
        for (; i < offset + size && j < content.length; i++, j++) {
          dest.put(i, content[j]);
        }
    }
  }

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
