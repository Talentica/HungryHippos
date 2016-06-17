package com.talentica.hungryHippos.client.data.parser;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.DataLocator.DataType;
import com.talentica.hungryHippos.client.domain.DataTypes;
import com.talentica.hungryHippos.client.domain.InvalidRowException;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.client.domain.MutableDouble;
import com.talentica.hungryHippos.client.domain.MutableInteger;
import com.talentica.hungryHippos.client.domain.MutableFloat;
import com.talentica.hungryHippos.client.domain.MutableLong;
import com.talentica.hungryHippos.client.validator.CsvParserValidator;
import com.talentica.hungryHippos.client.validator.DataParserValidator;
import com.talentica.hungryHippos.client.validator.InvalidStateException;

public class CsvDataParser extends LineByLineDataParser {

  private DataTypes[] buffer;
  private int numfields;
  private InvalidRowException invalidRow = new InvalidRowException("Invalid Row");

  private DataDescription dataDescription;
  boolean[] columnsStatusForInvalidRow = null;
  private int fieldIndex = 0;
  private int countDoubleQuotesInField = 0;

  public CsvDataParser(DataDescription dataDescription) {
    super(dataDescription);
    initializeMutableArrayStringBuffer(dataDescription);
    setDataDescription(dataDescription);
  }

  @Override
  public DataTypes[] processLine(MutableCharArrayString data) {
    boolean isInvalidRow = false;
    resetFieldIndex();
    setDataDescription(dataDescription);
    for (DataTypes s : buffer) {
      s.reset();
    }
    csvValidator.startFieldValidation(); // Start the field validation.
    char[] characters = data.getUnderlyingCharArray();
    for (int pointer = 0; pointer < data.length(); pointer++) {
      char nextChar = getCharacter(characters, pointer);
      try {
        if (csvValidator.isSeparator(nextChar)) {
          csvValidator.startFieldValidation();
          incrementFieldIndex();
          if (csvValidator.isEnabledDoubleQuoteChar()) {
            resetDoubleQuoteCount();
          }
        } else if (csvValidator.isDoubleQuoteChar(nextChar)
            && csvValidator.isEnabledDoubleQuoteChar()) {
          countDoubleQuotesInField++;
          char nextToNextChar = getCharacter(characters, (pointer + 1)); // check for next char
                                                                         // after second double
                                                                         // quote.
          if (csvValidator.isDoubleQuoteChar(nextToNextChar)) {
            countDoubleQuotesInField++;
            fillCharInBuffer(nextToNextChar);
            pointer++;
            continue;
          } else if (csvValidator.isSeparator(nextToNextChar)) {
            if (checkDoubleQuotePairsFound()) {
              csvValidator.stopFieldValidation();
            }
            continue;
          }
          if (csvValidator.isRetainOuterDoubleQuotes()) {
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
          char nextToNextChar = getCharacter(characters, (pointer + 1));
          if (csvValidator.isSeparator(nextToNextChar)) {
            csvValidator.stopFieldValidation();
          }
        }
      } catch (ArrayIndexOutOfBoundsException | InvalidStateException ex) {
        resetDoubleQuoteCount();
        csvValidator.stopFieldValidation();
        isInvalidRow = setInvalidRow(isInvalidRow);
        pointer = markFieldAndSkip(characters, pointer);
      }
    }
    try {
      csvValidator.stopFieldValidation();
    } catch (InvalidStateException ex) {
      resetDoubleQuoteCount();
      isInvalidRow = setInvalidRow(isInvalidRow);
    }

    if (isInvalidRow) {
      invalidRow.setColumns(columnsStatusForInvalidRow);
      invalidRow.setBadRow(data);
      throw invalidRow;
    }
    return buffer;
  }

  private boolean setInvalidRow(boolean isInvalidRow) {
    if (!isInvalidRow) {
      resetRowStatus();
      isInvalidRow = true;
    }
    return isInvalidRow;
  }

  private int markFieldAndSkip(char[] characters, int pointer) {
    columnsStatusForInvalidRow[fieldIndex] = true;
    pointer = skipNextChars(characters, pointer);
    return pointer;
  }

  private int skipNextChars(char[] characters, int pointer) {
    char skipChar = getCharacter(characters, pointer);
    while (!csvValidator.isSeparator(skipChar)) {
      pointer++;
      skipChar = getCharacter(characters, pointer);
    }
    return pointer - 1;
  }

  private char getCharacter(char[] characters, int pointer) {
    return characters[pointer];
  }

  private void fillCharInBuffer(char nextChar) {
    buffer[fieldIndex].addByte((byte) nextChar);
  }

  private void incrementFieldIndex() {
    fieldIndex++;
  }

  private void resetFieldIndex() {
    fieldIndex = 0;
  }

  private boolean checkDoubleQuotePairsFound() {
    return (countDoubleQuotesInField != 0 && countDoubleQuotesInField % 2 == 0
        && csvValidator.isEnabledDoubleQuoteChar());
  }

  private void resetDoubleQuoteCount() {
    if (csvValidator.isEnabledDoubleQuoteChar())
      countDoubleQuotesInField = 0;
  }

  private void initializeMutableArrayStringBuffer(DataDescription dataDescription) {
    numfields = dataDescription.getNumberOfDataFields();
    buffer = new DataTypes[numfields];
    for (int i = 0; i < numfields; i++) {
      DataLocator dataLocator = dataDescription.locateField(i);
      int numberOfCharsDataTypeTakes = dataLocator.getSize();
      /*
       * // TODO: Need to fix hard coding later. if (dataLocator.getDataType() == DataType.DOUBLE ||
       * dataLocator.getDataType() == DataType.INT || dataLocator.getDataType() == DataType.LONG ||
       * dataLocator.getDataType() == DataType.LONG) { numberOfCharsDataTypeTakes = 25; }
       */
      createBuffer(dataLocator.getDataType(), i, numberOfCharsDataTypeTakes);
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

  private void resetRowStatus() {
    for (int fieldNum = 0; fieldNum < columnsStatusForInvalidRow.length; fieldNum++) {
      columnsStatusForInvalidRow[fieldNum] = false;
    }
  }

  @Override
  public DataParserValidator createDataParserValidator() {
    return new CsvParserValidator(',', '\0', '\\', false, false);

  }

  /**
   * This method is used for creating buffer on the basis of dataType. The values are hardcoded as
   * Double can store almost 308 byte of data float can store 38 byte of data.
   * 
   * @param type
   * @param i
   * @param size
   */
  private void createBuffer(DataType type, int i, int size) {
    switch (type) {
      // 308
      case DOUBLE:
        size = 30;
        buffer[i] = new MutableDouble(size);
        break;
      // 38
      case FLOAT:
        size = 25;
        buffer[i] = new MutableFloat(size);
        break;
      case INT:
        size = 10;
        buffer[i] = new MutableInteger(size);
        break;
      case LONG:
        size = 19;
        buffer[i] = new MutableLong(size);
        break;
      // default String
      default:
        buffer[i] = new MutableCharArrayString(size);
        break;

    }

  }
}
