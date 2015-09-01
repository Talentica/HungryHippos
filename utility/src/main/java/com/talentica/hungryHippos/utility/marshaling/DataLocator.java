package com.talentica.hungryHippos.utility.marshaling;

/**
 * Created by debasishc on 1/9/15.
 */
public class DataLocator {
    public static enum DataType{
        BYTE, SHORT, INT, LONG, CHAR, FLOAT, DOUBLE, STRING
    }
    private int offset;
    private int size;
    private DataType dataType;

    public DataLocator(DataType dataType, int offset, int size) {
        this.dataType = dataType;
        this.offset = offset;
        this.size = size;
    }


    public DataType getDataType() {
        return dataType;
    }

    public int getOffset() {
        return offset;
    }

    public int getSize() {
        return size;
    }
}
