package com.talentica.hungryHippos.utility.marshaling;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by debasishc on 1/9/15.
 */
public class FieldTypeArrayDataDescription implements DataDescription{

    Map<Integer, DataLocator> dataLocatorMap = new HashMap<>();
    int nextIndex=0;
    int nextOffset=0;
    @Override
    public DataLocator locateField(int index) {
        return dataLocatorMap.get(index);
    }

    @Override
    public int getSize() {
        return nextOffset;
    }

    public void addFieldType(DataLocator.DataType dataType, int size){

        switch(dataType){
            case BYTE:
                size=1;
                break;
            case SHORT:
                size=2;
                break;
            case INT:
                size=4;
                break;
            case LONG:
                size=8;
                break;
            case CHAR:
                size=2;
                break;
            case FLOAT:
                size=4;
                break;
            case DOUBLE:
                size=8;
                break;
        }
        DataLocator locator = new DataLocator(dataType, nextOffset, size);
        dataLocatorMap.put(nextIndex,locator);
        nextIndex++;
        nextOffset+=size;
    }
}
