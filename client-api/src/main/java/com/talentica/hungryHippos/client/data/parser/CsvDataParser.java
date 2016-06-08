package com.talentica.hungryHippos.client.data.parser;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.DataLocator.DataType;
import com.talentica.hungryHippos.client.domain.InvalidRowExeption;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;

public class CsvDataParser extends LineByLineDataParser {

	private MutableCharArrayString[] buffer;

	private int numfields;

	private DataDescription dataDescription;
	
	private InvalidRowExeption invalidRow = new InvalidRowExeption("Invalid Row");
	
	boolean[] columnsStatusForInvalidRow = null;

	public CsvDataParser() {
	}

	public CsvDataParser(DataDescription dataDescription) {
		setDataDescription(dataDescription);
	}

	@Override
	public MutableCharArrayString[] processLine(MutableCharArrayString data, DataDescription dataDescription)
			throws InvalidRowExeption {
		boolean isInvalidRow = false; 
		setDataDescription(dataDescription);
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
				try {
					buffer[fieldIndex].addCharacter(nextChar);
				} catch (ArrayIndexOutOfBoundsException ex) {
					if(!isInvalidRow){ 
						resetRowStatus();
						isInvalidRow = true;
						}
					columnsStatusForInvalidRow[fieldIndex] = true;
				}
			}
		}
		if (isInvalidRow) {
			invalidRow.setColumns(columnsStatusForInvalidRow);
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
		columnsStatusForInvalidRow = new boolean[buffer.length];
	}

	@Override
	protected int getMaximumSizeOfSingleBlockOfDataInBytes(DataDescription dataDescription) {
		setDataDescription(dataDescription);
		return dataDescription.getMaximumSizeOfSingleBlockOfData();
	}

	private void setDataDescription(DataDescription dataDescription) {
		if (this.dataDescription == null) {
			this.dataDescription = dataDescription;
			initializeMutableArrayStringBuffer(dataDescription);
		}
	}

	private void  resetRowStatus(){
		for(int fieldNum = 0 ; fieldNum < columnsStatusForInvalidRow.length ; fieldNum++){
			columnsStatusForInvalidRow[fieldNum] = false;
		}
	}
}
