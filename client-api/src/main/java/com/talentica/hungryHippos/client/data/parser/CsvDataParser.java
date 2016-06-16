package com.talentica.hungryHippos.client.data.parser;

import com.talentica.hungryHippos.client.data.parser.context.CSVParseException;
import com.talentica.hungryHippos.client.data.parser.context.Context;
import com.talentica.hungryHippos.client.data.parser.context.ParseState;
import com.talentica.hungryHippos.client.data.parser.context.SpecialCharacter;
import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.DataLocator.DataType;
import com.talentica.hungryHippos.client.domain.InvalidRowException;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;

/**
 * 
 * A CsvDataParser builds for representation of a CSV file by ingesting one character at a time from
 * row under process. This representation is stored in the context. A csv data parser maintains the
 * state and performs the transitions of the DFA which accepts the CSV language.
 * 
 * @author nitink
 * @author pooshans
 *
 */
public class CsvDataParser extends LineByLineDataParser {

  private MutableCharArrayString[] buffer;

  private int numfields;

  private InvalidRowException invalidRow = new InvalidRowException("Invalid Row");

  boolean[] columnsStatusForInvalidRow = null;

  private final Context pContext;

  private ParseState pState;

  private int pLine = 1;

  private int columnPosition = 1;

  private char[] characters;

  private int columnPointer = 0;

  public CsvDataParser(DataDescription dataDescription) {
    super(dataDescription);
    initializeMutableArrayStringBuffer(dataDescription);
    pContext = new Context(buffer);
  }

  @Override
  public MutableCharArrayString[] processLine(MutableCharArrayString data) {
    pState = ParseState.START;
    pContext.resetBuffer();
    boolean isInvalidRow = false;
    int fieldIndex = 0;
    columnPointer = 0;
    characters = data.getUnderlyingArray();
    for (int pointer = 0; pointer < data.length(); pointer++) {
      columnPointer = pointer;
      try {
        char nextChar = characters[pointer];
        if (nextChar == SpecialCharacter.COMMA.getRepresentation()) {
          pContext.pushToken();
          fieldIndex++;
        }
        parseCharacter(nextChar);
      } catch (ArrayIndexOutOfBoundsException | CSVParseException ex) {
        pContext.clearTokenBuffer();
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
    return pContext.getParsedRow();
  }

  /**
   * Accept character of the row under process to get validated/interpreted in CSV language.
   * 
   * @param character
   * @throws CSVParseException
   * @throws ArrayIndexOutOfBoundsException
   */
  public void parseCharacter(final char character) throws CSVParseException,
      ArrayIndexOutOfBoundsException {
    switch (pState) {
      case START:
        start(character);
        break;
      case END:
        end(character);
        break;
      case UNQUOTED_TOKEN:
        unquotedToken(character);
        break;
      case QUOTED_TOKEN:
        quotedToken(character);
        break;
      case ESCAPED_QUOTE:
        escapedQuote(character);
        break;
      case QUOTED_TOKEN_SPACE_TRAIL:
        quotedTokenSpaceTrail(character);
        break;
      default:
        throw new RuntimeException("Unexpected state");
    }
  }


  /**
   * Transition from the ESCAPED_QUOTE state to other states, per the DFA.
   *
   * @param character in the CSV file.
   * @throws CSVParseException if character can not be correctly parsed.
   */
  private void escapedQuote(char character) throws CSVParseException {
    columnPosition++;
    if ((SpecialCharacter.SPACE.getRepresentation() == character)
        || (SpecialCharacter.TAB.getRepresentation() == character)) {
      pState = ParseState.QUOTED_TOKEN_SPACE_TRAIL;
    } else if ((SpecialCharacter.LINE_FEED.getRepresentation() == character)
        || (SpecialCharacter.CARRIAGE_RETURN.getRepresentation() == character)) {
      pContext.pushToken();
      pState = ParseState.END;
    } else if ((SpecialCharacter.COMMA.getRepresentation() == character)) {
      pState = ParseState.START;
    } else if ((SpecialCharacter.QUOTE.getRepresentation() == character && characters[columnPointer + 1] != SpecialCharacter.COMMA
        .getRepresentation())) {
      pContext.pushTokenChar(SpecialCharacter.QUOTE.getRepresentation());
      pState = ParseState.QUOTED_TOKEN;
    } else {
      pState = ParseState.START;
      throw new CSVParseException("Unexpected character after end quote.", pLine, columnPosition,
          character);
    }
  }

  /**
   * Transition from the QUOTED_TOKEN_SPACE_TRAIL state to other states, per the DFA.
   *
   * @param character in the CSV file.
   */
  private void quotedTokenSpaceTrail(char character) {
    columnPosition++;
    if ((SpecialCharacter.SPACE.getRepresentation() == character)
        || (SpecialCharacter.TAB.getRepresentation() == character)) {
      pState = ParseState.QUOTED_TOKEN_SPACE_TRAIL;
    } else if ((SpecialCharacter.LINE_FEED.getRepresentation() == character)
        || (SpecialCharacter.CARRIAGE_RETURN.getRepresentation() == character)) {
      pState = ParseState.END;
    } else if ((SpecialCharacter.COMMA.getRepresentation() == character)) {
      pState = ParseState.START;
    } else {
      pState = ParseState.START;
      throw new IllegalArgumentException("Unexpected letter after end quote: " + character);
    }
  }

  /**
   * Transition from the QUOTED_TOKEN state to other states, per the DFA.
   *
   * @param character in the CSV file.
   */
  private void quotedToken(char character) {
    columnPosition++;
    if ((SpecialCharacter.QUOTE.getRepresentation() == character)) {
      pState = ParseState.ESCAPED_QUOTE;
    } else {
      pContext.pushTokenChar(character);
      pState = ParseState.QUOTED_TOKEN;
    }
  }

  /**
   * Transition from the UNQUOTED_TOKEN state to other states, per the DFA.
   *
   * @param character in the CSV file.
   * @throws CSVParseException if character can not be correctly parsed.
   */
  private void unquotedToken(char character) throws CSVParseException {
    columnPosition++;
    if ((SpecialCharacter.SPACE.getRepresentation() == character)
        || (SpecialCharacter.TAB.getRepresentation() == character)) {
      pContext.pushSpace(character);
      pState = ParseState.UNQUOTED_TOKEN;
    } else if ((SpecialCharacter.LINE_FEED.getRepresentation() == character)
        || (SpecialCharacter.CARRIAGE_RETURN.getRepresentation() == character)) {
      pContext.pushToken();
      pState = ParseState.END;
    } else if ((SpecialCharacter.COMMA.getRepresentation() == character)) {
      pState = ParseState.START;
    } else if ((SpecialCharacter.QUOTE.getRepresentation() == character)) {
      pState = ParseState.START;
      throw new CSVParseException("Unexpected quote in the middle of an unquoted token.", pLine,
          columnPosition, character);
    } else {
      pContext.pushSpaceTrail();
      pContext.pushTokenChar(character);
      pState = ParseState.UNQUOTED_TOKEN;
    }
  }

  /**
   * Transition from the END state to other states, per the DFA.
   *
   * @param character in the CSV file.
   */
  private void end(char character) {
    if ((SpecialCharacter.LINE_FEED.getRepresentation() == character)
        || (SpecialCharacter.CARRIAGE_RETURN.getRepresentation() == character)) {
      pState = ParseState.END;
    } else {
      pLine++;
      columnPosition = 0;
      start(character);
    }
  }

  /**
   * Transition from the START state to other states, per the DFA.
   *
   * @param character in the CSV file.
   */
  private void start(char character) {
    columnPosition++;
    if ((SpecialCharacter.SPACE.getRepresentation() == character)
        || (SpecialCharacter.TAB.getRepresentation() == character)) {
      pState = ParseState.START;
    } else if ((SpecialCharacter.LINE_FEED.getRepresentation() == character)
        || (SpecialCharacter.CARRIAGE_RETURN.getRepresentation() == character)) {
      pContext.pushToken();
      pState = ParseState.END;
    } else if ((SpecialCharacter.COMMA.getRepresentation() == character)) {
      pState = ParseState.START;
    } else if ((SpecialCharacter.QUOTE.getRepresentation() == character)) {
      pState = ParseState.QUOTED_TOKEN;
    } else {
      pContext.pushTokenChar(character);
      pState = ParseState.UNQUOTED_TOKEN;
    }
  }

  public Context getContext() {
    return pContext;
  }

  public ParseState getParserState() {
    return pState;
  }

  private void initializeMutableArrayStringBuffer(DataDescription dataDescription) {
    numfields = dataDescription.getNumberOfDataFields();
    buffer = new MutableCharArrayString[numfields];
    for (int i = 0; i < numfields; i++) {
      DataLocator dataLocator = dataDescription.locateField(i);
      int numberOfCharsDataTypeTakes = dataLocator.getSize();
      // TODO: Need to fix hard coding later.
      if (dataLocator.getDataType() == DataType.DOUBLE || dataLocator.getDataType() == DataType.INT
          || dataLocator.getDataType() == DataType.LONG
          || dataLocator.getDataType() == DataType.FLOAT) {
        numberOfCharsDataTypeTakes = 25;
      }
      buffer[i] = new MutableCharArrayString(numberOfCharsDataTypeTakes);
    }
    columnsStatusForInvalidRow = new boolean[buffer.length];
  }

  private void resetRowStatus() {
    for (int fieldNum = 0; fieldNum < columnsStatusForInvalidRow.length; fieldNum++) {
      columnsStatusForInvalidRow[fieldNum] = false;
    }
  }
}
