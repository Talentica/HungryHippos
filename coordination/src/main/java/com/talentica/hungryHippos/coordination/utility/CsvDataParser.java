package com.talentica.hungryHippos.coordination.utility;

import com.talentica.hungryHippos.client.data.DataParser;
import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.DataLocator.DataType;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.InvalidRowExeption;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;

public class CsvDataParser implements DataParser {

	private MutableCharArrayString[] buffer;

	private int numfields;

	private DataDescription dataDescription;

	public CsvDataParser() {
		this(FieldTypeArrayDataDescription.createDataDescription(Property.getDataTypeConfiguration(),
				Property.getMaximumSizeOfSingleDataBlock()));
	}

	public CsvDataParser(DataDescription dataDescription) {
		this.dataDescription = dataDescription;
		initializeMutableArrayStringBuffer(dataDescription);
	}

	@Override
	public MutableCharArrayString[] preprocess(MutableCharArrayString data) throws InvalidRowExeption {
		InvalidRowExeption invalidRow = null;
		boolean[] columns = null;
		for (MutableCharArrayString s : buffer) {
			s.reset();
		}
		int fieldIndex = 0;
		char[] characters = data.getUnderlyingArray();
		for (int i = 0; i < data.length(); i++) {
			char nextChar = characters[i];
			if (nextChar == ',') {
				fieldIndex++;
			} else {
				try{
				buffer[fieldIndex].addCharacter(nextChar);
				}catch(ArrayIndexOutOfBoundsException ex){
					if(invalidRow == null){
						columns = new boolean[buffer.length];
						invalidRow = new InvalidRowExeption("Invalid Row");
					}
					columns[fieldIndex] = true;
				}
			}
		}
		if(invalidRow != null){
		invalidRow.setColumns(columns);
		invalidRow.setBadRow(data);
		throw invalidRow;
		}
		return buffer;
	}

	private void initializeMutableArrayStringBuffer(DataDescription dataDescription) {
		numfields = dataDescription.getNumberOfDataFields();
		buffer = new MutableCharArrayString[numfields];
		for (int i = 0; i < numfields; i++) {
			DataLocator dataLocator = dataDescription.locateField(i);
			int numberOfCharsDataTypeTakes = dataLocator.getSize();
			// TODO: Need to fix hard coding later.
			if (dataLocator.getDataType() == DataType.DOUBLE || dataLocator.getDataType() == DataType.INT
					|| dataLocator.getDataType() == DataType.LONG || dataLocator.getDataType() == DataType.FLOAT) {
				numberOfCharsDataTypeTakes = 25;
			}
			buffer[i] = new MutableCharArrayString(numberOfCharsDataTypeTakes);
		}
	}

	@Override
	public DataDescription getDataDescription() {
		return dataDescription;
	}

}
