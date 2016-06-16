package com.talentica.hungryHippos.client.data.parser.context;

import java.io.IOException;

/**
 *All CSV parsing exceptions.
 */
public class CSVParseException extends IOException {

  /**
   * Unnecessary.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Construct exception with line, position, offending character, and message.
   *
   * @param message to explain exception.
   * @param invalidLine where character can not be correctly parsed.
   * @param invalidPosition of the character which can not be correctly parsed.
   * @param invalidCharacter which can not be correctly parsed.
   */
  public CSVParseException(final String message,
      final int invalidLine,
      final int invalidPosition,
      final char invalidCharacter) {
    super(String.format("On line %d, position %d, encountered %s. %s",
        invalidLine,
        invalidPosition,
        invalidCharacter,
        message));
  }

  /**
   * Construct exception with message.
   *
   * @param message to explain exception.
   */
  public CSVParseException(final String message) {
    super(message);
  }
}
