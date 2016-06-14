/**
 * 
 */
package com.talentica.hungryHippos.client.validator;

import java.util.HashMap;
import java.util.Map;

/**
 * Parsing of the CSV file is performed by providing different element of the validator which is
 * required to filter the values.
 * 
 * @author pooshans
 *
 */
public class CsvParserValidator implements DataParserValidator {

  private char separator;
  private char doubleQuotechar;
  private char escapechar;;
  private boolean isTrimWhiteSpace;
  private boolean isRetainOuterQuotes;
  private char[] lineSeperator;
  boolean isFieldValidationStarted = false;
  boolean isEnableDoubleQuoteChar;
  private static Map<ExceptionEnum, InvalidStateException> exceptionObjPool;

  enum ExceptionEnum {
    EXCEPTION_START_FIELD, EXCEPTION_STOP_FIELD;
  }

  public CsvParserValidator() {
    this.separator = DEFAULT_SEPARATOR;
    this.doubleQuotechar = DEFAULT_DOUBLE_QUOTE_CHAR;
    this.escapechar = DEFAULT_ESCAPE_CHAR;
    this.isTrimWhiteSpace = DEFAULT_TRIM_WS;
    this.isRetainOuterQuotes = DEFAULT_RETAIN_OUTER_QUOTES;
    this.lineSeperator = DEFAULT_LINE_SEPARATOR_CHARS;
    this.isEnableDoubleQuoteChar = isEnabledDoubleQuoteChar();
  }

  /**
   * Parameterized Constructor accepting the necessary overriding default values.
   * 
   * @param separator is character separator for values in lines. i.e ',' or '\t' etc.
   * @param quotechar is value token under this character.
   * @param escapechar is to retain the special character in value.
   * @param isTrimWhiteSpace is to trim the leading or trailing whitespace while parsing the value
   *        token.
   * @param isRetainOuterQuotes
   * @param lineSeparator can be windows "\r\n" or unix "\n" in terms of char[]{13,10} or char[10]
   *        which represents the ASCII values. respectively.
   */
  public CsvParserValidator(final char separator, final char quotechar, final char escapechar,
      final boolean isTrimWhiteSpace, final boolean isRetainOuterQuotes) {
    if (NULL_CHARACTER != separator) {
      this.separator = separator;
    } else {
      this.separator = DEFAULT_SEPARATOR;
    }
    if (NULL_CHARACTER != quotechar) {
      this.doubleQuotechar = quotechar;
    }
    if (NULL_CHARACTER != escapechar) {
      this.escapechar = escapechar;
    } else {
      this.escapechar = DEFAULT_ESCAPE_CHAR;
    }
    this.isTrimWhiteSpace = isTrimWhiteSpace;
    this.isRetainOuterQuotes = isRetainOuterQuotes;
    this.lineSeperator = DEFAULT_LINE_SEPARATOR_CHARS;
    this.isEnableDoubleQuoteChar = isEnabledDoubleQuoteChar();
  }

  public char getSeparator() {
    return separator;
  }

  public void setSeparator(char separator) {
    this.separator = separator;
  }

  public char getDoubleQuotechar() {
    return doubleQuotechar;
  }

  public void setDoubleQuotechar(char doubleQuotechar) {
    this.doubleQuotechar = doubleQuotechar;
  }

  public char getEscapechar() {
    return escapechar;
  }

  public void setEscapechar(char escapechar) {
    this.escapechar = escapechar;
  }

  public boolean isTrimWhiteSpace() {
    return isTrimWhiteSpace;
  }

  public void setTrimWhiteSpace(boolean isTrimWhiteSpace) {
    this.isTrimWhiteSpace = isTrimWhiteSpace;
  }

  public boolean isRetainOuterDoubleQuotes() {
    return isRetainOuterQuotes;
  }

  public void setRetainOuterQuotes(boolean isRetainOuterQuotes) {
    this.isRetainOuterQuotes = isRetainOuterQuotes;
  }

  public boolean isDoubleQuoteChar(char character) {
    return (character == doubleQuotechar);
  }

  public boolean isSeparator(char character) {
    return (character == separator);
  }

  public boolean isEscapechar(char character) {
    return (character == escapechar);
  }

  @Override
  public char[] getLineSeparator() {
    return this.lineSeperator;
  }

  @Override
  public void startFieldValidation() {
    if (isFieldValidationStarted) {
      throw exceptionObjectPool(ExceptionEnum.EXCEPTION_START_FIELD);
    }
    isFieldValidationStarted = true;

  }

  @Override
  public void stopFieldValidation() {
    if (!isFieldValidationStarted) {
      throw exceptionObjectPool(ExceptionEnum.EXCEPTION_STOP_FIELD);
    }
    isFieldValidationStarted = false;
  }

  @Override
  public boolean isFieldValidationStarted() {
    return isFieldValidationStarted;
  }

  @Override
  public boolean isEnabledDoubleQuoteChar() {
    return (doubleQuotechar != NULL_CHARACTER);
  }

  private InvalidStateException exceptionObjectPool(ExceptionEnum exception) {

    if (exceptionObjPool == null) {
      exceptionObjPool = new HashMap<ExceptionEnum, InvalidStateException>();
    }

    InvalidStateException invalidStateException = exceptionObjPool.get(exception);
    if (invalidStateException == null) {
      switch (exception) {
        case EXCEPTION_START_FIELD:
          invalidStateException =
              new InvalidStateException("Field validation on current field is still in progress");
          exceptionObjPool.put(exception, invalidStateException);
          break;
        case EXCEPTION_STOP_FIELD:
          invalidStateException =
              new InvalidStateException("Field validation on current field is already stopped.");
          exceptionObjPool.put(exception, invalidStateException);
          break;
        default:
          break;
      }
    }
    return invalidStateException;
  }
}
