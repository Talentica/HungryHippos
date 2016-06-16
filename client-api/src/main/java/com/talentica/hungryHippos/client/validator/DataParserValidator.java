package com.talentica.hungryHippos.client.validator;

/**
 * DataParserValidator provides the leverage to the client to write their own Data Parser according
 * to the file under process. Default implementation is CSV parser.
 * 
 * @author pooshans
 *
 */
public interface DataParserValidator extends DataParserValidationContext{
  char DEFAULT_SEPARATOR = ',';
  char DEFAULT_DOUBLE_QUOTE_CHAR = '"';
  char DEFAULT_ESCAPE_CHAR = '\\';
  boolean DEFAULT_TRIM_WS = false;
  boolean DEFAULT_RETAIN_OUTER_QUOTES = false;
  char NULL_CHARACTER = '\0';
  char[] DEFAULT_LINE_SEPARATOR_CHARS = {13, 10};
  
  boolean isDoubleQuoteChar(char character);

  boolean isSeparator(char character);

  boolean isEscapechar(char character);

  boolean isRetainOuterDoubleQuotes();

  boolean isTrimWhiteSpace();
  
  char[] getLineSeparator();
  
boolean isFieldValidationStarted();

boolean isEnabledDoubleQuoteChar();

}
