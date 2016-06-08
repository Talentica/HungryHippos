package com.talentica.hungryHippos.client.data.parser;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.DataLocator.DataType;
import com.talentica.hungryHippos.client.domain.InvalidRowException;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.client.validator.CsvParserValidator;
import com.talentica.hungryHippos.client.validator.CsvValidator;

public class CsvDataParser extends LineByLineDataParser {

	private MutableCharArrayString[] buffer;
	private int numfields;
	private InvalidRowException invalidRow = new InvalidRowException(
			"Invalid Row");

	private DataDescription dataDescription;
	boolean[] columnsStatusForInvalidRow = null;
	private int fieldIndex = 0;

	public CsvDataParser(DataDescription dataDescription) {
		super(dataDescription);
		initializeMutableArrayStringBuffer(dataDescription);
		setDataDescription(dataDescription);
	}

	@Override
	public MutableCharArrayString[] processLine(MutableCharArrayString data) {
		boolean isInvalidRow = false;
		resetFieldIndex();
		setDataDescription(dataDescription);
		for (MutableCharArrayString s : buffer) {
			s.reset();
		}
		char[] characters = data.getUnderlyingArray();
		for (int pointer = 0; pointer < data.length(); pointer++) {
			try {
				char nextChar = getCharacter(characters, pointer);
				if (csvValidator.isSeparator(nextChar)) {
					incrementFieldIndex();
				} else if (csvValidator.isQuoteChar(nextChar)) {
					if (csvValidator.isRetainOuterQuotes()) {
						fillCharInBuffer(nextChar);
					}
					continue;
				} else if (csvValidator.isEscapechar(nextChar)) {
					pointer++;
					nextChar = getCharacter(characters, pointer);
					fillCharInBuffer(nextChar);
					continue;
				} else if (csvValidator.isTrimWhiteSpace()) {
					pointer++;
					continue;
				} else {
					fillCharInBuffer(nextChar);
				}
			} catch (ArrayIndexOutOfBoundsException ex) {
				if (!isInvalidRow) {
					resetRowStatus();
					isInvalidRow = true;
				}
				columnsStatusForInvalidRow[fieldIndex] = true;
			}
		}
		if (isInvalidRow) {
			invalidRow.setColumns(columnsStatusForInvalidRow);
			invalidRow.setBadRow(data);
			throw invalidRow;
		}
		return buffer;
	}

	private char getCharacter(char[] characters, int pointer) {
		return characters[pointer];
	}

	private void fillCharInBuffer(char nextChar) {
		buffer[fieldIndex].addCharacter(nextChar);
	}

	private void incrementFieldIndex() {
		fieldIndex++;
	}

	private void resetFieldIndex() {
		fieldIndex = 0;
	}

	private void initializeMutableArrayStringBuffer(
			DataDescription dataDescription) {
		numfields = dataDescription.getNumberOfDataFields();
		buffer = new MutableCharArrayString[numfields];
		for (int i = 0; i < numfields; i++) {
			DataLocator dataLocator = dataDescription.locateField(i);
			int numberOfCharsDataTypeTakes = dataLocator.getSize();
			// TODO: Need to fix hard coding later.
			if (dataLocator.getDataType() == DataType.DOUBLE
					|| dataLocator.getDataType() == DataType.INT
					|| dataLocator.getDataType() == DataType.LONG
					|| dataLocator.getDataType() == DataType.FLOAT) {
				numberOfCharsDataTypeTakes = 25;
			}
			buffer[i] = new MutableCharArrayString(numberOfCharsDataTypeTakes);
		}
		columnsStatusForInvalidRow = new boolean[buffer.length];
	}

	@Override
	protected int getMaximumSizeOfSingleBlockOfDataInBytes(
			DataDescription dataDescription) {
		setDataDescription(dataDescription);
		return dataDescription.getMaximumSizeOfSingleBlockOfData();
	}

	private void setDataDescription(DataDescription dataDescription) {
		if (this.dataDescription == null) {
			this.dataDescription = dataDescription;
			initializeMutableArrayStringBuffer(dataDescription);
		}
	}

	private void resetRowStatus() {
		for (int fieldNum = 0; fieldNum < columnsStatusForInvalidRow.length; fieldNum++) {
			columnsStatusForInvalidRow[fieldNum] = false;
		}
	}

	@Override
	public CsvValidator createDataParserValidator() {
		return new CsvParserValidator(',', '\0', '\\', false, false);

	}
}
