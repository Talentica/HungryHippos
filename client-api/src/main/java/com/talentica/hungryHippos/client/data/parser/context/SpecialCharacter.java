/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
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
