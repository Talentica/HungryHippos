package com.talentica.hungryHippos.utility.marshaling;

import java.io.Serializable;
import java.nio.ByteBuffer;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;

/**
 * Created by debasishc on 1/9/15.
 */
public class DynamicMarshal implements Serializable{
    /**
	 * 
	 */
	private static final long serialVersionUID = -5800537222182360030L;
	private DataDescription dataDescription;

    public DynamicMarshal(DataDescription dataDescription) {
        this.dataDescription = dataDescription;
    }

    public Object readValue(int index, ByteBuffer source){
        DataLocator locator = dataDescription.locateField(index);
        switch (locator.getDataType()){
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

    public MutableCharArrayString readValueString(int index, ByteBuffer source){
        DataLocator locator = dataDescription.locateField(index);
        int offset = locator.getOffset();
        int size = locator.getSize();
		MutableCharArrayString charArrayString = new MutableCharArrayString(size);
        for(int i=offset;i<offset+size;i++){
            byte ch = source.get(i);
            if(ch==0){
                break;
            }
			charArrayString.addCharacter((char) ch);
        }
        return charArrayString;
    }

    public void writeValue(int index, Object object, ByteBuffer dest) {
        DataLocator locator = dataDescription.locateField(index);
        switch (locator.getDataType()) {
            case BYTE:
                dest.put(locator.getOffset(), (Byte) object);
                break;
            case CHAR:
                dest.putChar(locator.getOffset(), (Character) object);
                break;
            case SHORT:
                dest.putShort(locator.getOffset(), (Short) object);
                break;
            case INT:
                dest.putInt(locator.getOffset(), (Integer) object);
                break;
            case LONG:
                dest.putLong(locator.getOffset(), (Long) object);
                break;
            case FLOAT:
                dest.putFloat(locator.getOffset(), (Float) object);
                break;
            case DOUBLE:
                dest.putDouble(locator.getOffset(), (Double) object);
                break;
            case STRING:
                byte[] content = object.toString().getBytes();
                int offset = locator.getOffset();
                int size = locator.getSize();

                int j = 0;
                int i = offset;
                for (; i < offset + size - 1 && j < content.length; i++, j++) {
                    dest.put(i, content[j]);
                }
                dest.put(i, (byte) 0);
        }
    }

    public void writeValueString(int index, MutableCharArrayString input, ByteBuffer dest){
        DataLocator locator = dataDescription.locateField(index);
        switch (locator.getDataType()){

            case STRING:
                int offset = locator.getOffset();
                int size = locator.getSize();

                int j=0;
                int i=offset;
                for(;i<offset+size-1 && j<input.length() ;i++,j++){
                    dest.put(i,(byte)input.charAt(j));
                }
                dest.put(i,(byte)0);
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
