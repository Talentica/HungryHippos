package com.talentica.hungryHippos.client.validator;

/**
 * @author pooshans
 *
 */
public interface DataParserValidator {
  char DEFAULT_SEPARATOR = ',';
  char DEFAULT_QUOTE_CHAR = '"';
  char DEFAULT_ESCAPE_CHAR = '\\';
  boolean DEFAULT_TRIM_WS = false;
  boolean DEFAULT_RETAIN_OUTER_QUOTES = false;
  char NULL_CHARACTER = '\0';
  char[] DEFAULT_LINE_SEPARATOR_CHARS = {13, 10};

  boolean isQuoteChar(char character);

  boolean isSeparator(char character);

  boolean isEscapechar(char character);

  boolean isRetainOuterQuotes();

  boolean isTrimWhiteSpace();

  char[] getLineSeparator();

}
