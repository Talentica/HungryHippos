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
