package com.talentica.test.parser;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

import com.talentica.hungryHippos.client.data.parser.LineByLineDataParser;
import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.DataLocator.DataType;
import com.talentica.hungryHippos.client.domain.DataTypes;
import com.talentica.hungryHippos.client.domain.InvalidRowException;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;

public class OddLinesProcessingCsvParser extends LineByLineDataParser {

	private int lineNumber = 0;

	private MutableCharArrayString[] buffer;
	
	FileInputStream dataInputStream ;

	private int numfields;

	private InvalidRowException invalidRow = new InvalidRowException("Invalid Row");

	boolean[] columnsStatusForInvalidRow = null;

	public OddLinesProcessingCsvParser(DataDescription dataDescription) throws FileNotFoundException {
		super(dataDescription);
		initializeMutableArrayStringBuffer(dataDescription);
		dataInputStream = new FileInputStream("File Path");
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
	protected DataTypes[] processLine(MutableCharArrayString line) {
		lineNumber++;
		if (lineNumber % 2 == 0) {
			if (iterator(dataInputStream).hasNext()) {
				return iterator(dataInputStream).next();
			}
		} else {
			return processOddLine(line);
		}
		return null;
	}

	private MutableCharArrayString[] processOddLine(MutableCharArrayString oddLine) {
		boolean isInvalidRow = false;
		for (MutableCharArrayString s : buffer) {
			s.reset();
		}
		int fieldIndex = 0;
		char[] characters = oddLine.getUnderlyingCharArray();
		for (int i = 0; i < oddLine.length(); i++) {
			char nextChar = characters[i];
			if (nextChar == ',') {
				fieldIndex++;
			} else {
				try {
					buffer[fieldIndex].addCharacter(nextChar);
				} catch (ArrayIndexOutOfBoundsException ex) {
					if (!isInvalidRow) {
						resetRowStatus();
						isInvalidRow = true;
					}
					columnsStatusForInvalidRow[fieldIndex] = true;
				}
			}
		}
		if (isInvalidRow) {
			invalidRow.setColumns(columnsStatusForInvalidRow);
			invalidRow.setBadRow(oddLine);
			throw invalidRow;
		}
		return buffer;
	}

	private void resetRowStatus() {
		for (int fieldNum = 0; fieldNum < columnsStatusForInvalidRow.length; fieldNum++) {
			columnsStatusForInvalidRow[fieldNum] = false;
		}
	}

}