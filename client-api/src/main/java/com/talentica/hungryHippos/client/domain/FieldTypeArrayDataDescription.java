package com.talentica.hungryHippos.client.domain;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by debasishc on 1/9/15.
 */
public final class FieldTypeArrayDataDescription implements DataDescription, Serializable {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1551866958938738882L;
	private Map<Integer, DataLocator> dataLocatorMap = new HashMap<>();
    private int nextIndex=0;
    private int nextOffset=0;

    public void setKeyOrder(String[] keyOrder) {
        this.keyOrder = keyOrder;
    }

    private String[] keyOrder;

    @Override
    public DataLocator locateField(int index) {
        return dataLocatorMap.get(index);
    }

    @Override
    public int getSize() {
        return nextOffset;
    }

    @Override
    public String[] keyOrder() {
        return keyOrder;
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
