package com.talentica.hungryHippos.utility.marshaling;

import java.nio.ByteBuffer;

/**
 * Created by debasishc on 1/9/15.
 */
public class DynamicMarshal {
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
                StringBuilder sb = new StringBuilder();
                int offset = locator.getOffset();
                int size = locator.getSize();
                for(int i=offset;i<offset+size;i++){
                    byte ch = source.get(i);
                    if(ch==0){
                        break;
                    }
                    sb.append((char)ch);
                }
                return sb.toString();
        }
        return null;
    }

    public void writeValue(int index, Object object, ByteBuffer dest){
        DataLocator locator = dataDescription.locateField(index);
        switch (locator.getDataType()){
            case BYTE:
                dest.put(locator.getOffset(), (Byte)object);
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

                int j=0;
                int i=offset;
                for(;i<offset+size-1 && j<content.length;i++,j++){
                    dest.put(i,content[j]);
                }
                dest.put(i,(byte)0);
        }

    }
}
