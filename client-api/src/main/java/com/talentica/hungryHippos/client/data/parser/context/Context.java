/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
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

import com.talentica.hungryHippos.client.domain.DataTypes;


/**
 * A Context represents a partially parsed CSV file in memory. It can build tokens by pushing
 * characters. It can build rows by pushing tokens. It can build a list of rows by pushing rows.
 * @author pooshans
 */
public class Context {

  public static final char[] WINDOWS_LINE_SEPARATOR_CHARS = {13, 10};

  private DataTypes[] buffer;

  public Context(DataTypes[] buffer) {
    this.buffer = buffer;
  }

  private int fieldIndex = 0;

  /**
   * Latest token being built.
   */
  private String tokenBuffer = "";

  /**
   * Keep track of trailing spaces and tabs until they can be discarded or appended to the token as
   * necessary.
   */
  private String spaceTrailBuffer = "";

  /**
   * Append the a letter to the latest token being built.
   *
   * @param letter to append.
   */
  public void pushTokenChar(final char letter) {
    tokenBuffer += letter;
  }

  public void pushToken() {
    buffer[fieldIndex].reset();
    buffer[fieldIndex].addValue(tokenBuffer);
    clearTokenBuffer();
    fieldIndex++;
  }

  public void clearTokenBuffer() {
    tokenBuffer = "";
  }

  /**
   * Add space or tab to the trailing space buffer. These space may or may not be part of the token.
   *
   * @param space or tab to push to the trailing space buffer.
   */
  public void pushSpace(final char space) {
    spaceTrailBuffer += space;
  }

  /**
   * The trailing space buffer ought to be pushed to the token.
   */
  public void pushSpaceTrail() {
    tokenBuffer += spaceTrailBuffer;
    clearSpaceTrail();
  }

  /**
   * Discard the trailing space buffer by clearing it.
   */
  public void clearSpaceTrail() {
    spaceTrailBuffer = "";
  }

  /**
   * Gets the token being built.
   *
   * @return token being built.
   */
  public String getTokenBuffer() {
    return tokenBuffer;
  }

  public DataTypes[] getParsedRow() {
    return buffer;
  }

  public void resetBuffer() {
    for (DataTypes s : buffer) {
      s.reset();
    }
    fieldIndex = 0;
  }

}
