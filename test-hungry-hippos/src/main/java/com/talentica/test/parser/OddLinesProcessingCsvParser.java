package com.talentica.test.parser;

import com.talentica.hungryHippos.client.data.parser.LineByLineDataParser;
import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.DataLocator.DataType;
import com.talentica.hungryHippos.client.domain.InvalidRowException;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.client.validator.CsvParserValidator;
import com.talentica.hungryHippos.client.validator.CsvValidator;

public class OddLinesProcessingCsvParser extends LineByLineDataParser {

	private int lineNumber = 0;

	private MutableCharArrayString[] buffer;

	private int numfields;

	private InvalidRowException invalidRow = new InvalidRowException("Invalid Row");

	boolean[] columnsStatusForInvalidRow = null;

	public OddLinesProcessingCsvParser(DataDescription dataDescription) {
		super(dataDescription);
		initializeMutableArrayStringBuffer(dataDescription);
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
	protected MutableCharArrayString[] processLine(MutableCharArrayString line) {
		lineNumber++;
		if (lineNumber % 2 == 0) {
			if (getIterator().hasNext()) {
				return getIterator().next();
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
		char[] characters = oddLine.getUnderlyingArray();
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

	@Override
	protected int getMaximumSizeOfSingleBlockOfDataInBytes(
			DataDescription dataDescription) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public CsvValidator createDataParserValidator() {
		return new CsvParserValidator(',', '\0', '\\', false, false);
	}

}