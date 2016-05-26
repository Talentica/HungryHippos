package com.talentica.hungryHippos.client.domain;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.xml.internal.ws.util.StringUtils;

/**
 * Created by debasishc on 1/9/15.
 */
public final class FieldTypeArrayDataDescription implements DataDescription, Serializable {

	private static final Logger LOGGER = LoggerFactory.getLogger(FieldTypeArrayDataDescription.class.getName());

	private static final long serialVersionUID = 1551866958938738882L;
	private Map<Integer, DataLocator> dataLocatorMap = new HashMap<>();
    private int nextIndex=0;
    private int nextOffset=0;

    public void setKeyOrder(String[] keyOrder) {
        this.keyOrder = keyOrder;
    }

	public FieldTypeArrayDataDescription() {

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

	public static final FieldTypeArrayDataDescription createDataDescription(String[] dataTypeConfiguration) {
		FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription();
		String[] datatype_size;
		String datatype;
		if (dataTypeConfiguration == null || dataTypeConfiguration.length == 0) {
			LOGGER.warn("\n\t No data type is specified in the configuration file");
			return dataDescription;
		}
		for (int index = 0; index < dataTypeConfiguration.length; index++) {
			int size=0;
			datatype_size = dataTypeConfiguration[index].split("-");
			datatype = datatype_size[0];
			if(datatype_size.length>1){
				size = Integer.valueOf(datatype_size[1]);
			}
			switch (datatype) {
			case "STRING":
				dataDescription.addFieldType(DataLocator.DataType.STRING, size);
				break;
			case "DOUBLE":
				dataDescription.addFieldType(DataLocator.DataType.DOUBLE, size);
				break;
			case "FLOAT":
				dataDescription.addFieldType(DataLocator.DataType.DOUBLE, size);
				break;
			case "CHAR":
				dataDescription.addFieldType(DataLocator.DataType.CHAR, size);
				break;
			case "LONG":
				dataDescription.addFieldType(DataLocator.DataType.LONG, size);
				break;
			case "INT":
				dataDescription.addFieldType(DataLocator.DataType.INT, size);
				break;
			case "SHORT":
				dataDescription.addFieldType(DataLocator.DataType.SHORT, size);
				break;
			case "BYTE":
				dataDescription.addFieldType(DataLocator.DataType.BYTE, size);
				break;
			default:
				LOGGER.info("\n\t {} datatype is undefined", datatype);
			}
		}
		return dataDescription;
	}

	@Override
	public FieldTypeArrayDataDescription clone() {
		FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription();
		if (dataLocatorMap != null) {
			dataDescription.dataLocatorMap.putAll(dataLocatorMap);
			dataDescription.nextIndex = nextIndex;
			dataDescription.nextOffset = nextOffset;
			dataDescription.keyOrder = keyOrder;
		}
		return dataDescription;
	}

	@Override
	public int getNumberOfDataFields() {
		return dataLocatorMap.size();
	}

}
