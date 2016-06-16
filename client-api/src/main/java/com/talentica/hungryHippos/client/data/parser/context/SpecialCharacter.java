package com.talentica.hungryHippos.client.data.parser.context;

/**
 * Characters having special representation in CSV file format under processing.
 */
public enum SpecialCharacter {
  /**
   * Space.
   */
  SPACE(' '),

  /**
   * Tab. Treat the same as space.
   */
  TAB('\t'),

  /**
   * Line feed terminates UNIX lines.
   */
  LINE_FEED('\n'),

  /**
   * Carriage return terminate legacy OS lines.
   */
  CARRIAGE_RETURN('\r'),

  /**
   * Double quote is used for complex string tokens.
   */
  QUOTE('"'),

  /**
   * Comma separates tokens.
   */
  COMMA(',');

  /**
   * String representation of special character.
   */
  private final char representation;

  /**
   * Private enum constructor.
   *
   * @param representation character of the instance.
   */
  private SpecialCharacter(final char representation) {
    this.representation = representation;
  }

  /**
   * Returns the special character of the instance.
   *
   * @return special character string representation.
   */
  public char getRepresentation() {
    return representation;
  }
}
