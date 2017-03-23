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
/**
 * 
 */
package com.talentica.hungryHippos.client.domain;

import java.util.Arrays;

/**
 * {@code InvalidRowException} is a subclass of {@code RuntimeException}. This exception implies
 * that while parsing the file. The system found contents which doesn't follow the DataDescription
 * that was provided during the initial phase of the system.
 * 
 * @author pooshans
 */
public class InvalidRowException extends RuntimeException {

  private static final long serialVersionUID = 1L;
  private String message;
  private MutableCharArrayString row;
  private boolean[] columns;

  /**
   * Constructs new InvalidRowException with the specified detail message.
   * 
   * @param message
   */
  public InvalidRowException(String message) {
    super(message);
    this.message = message;
  }

  /**
   * Constructs new InvalidRowException with the specified detail message and the column where the
   * system found wrong data type.
   * 
   * @param message contains the reason why this exception occurred.
   * @param columns is a boolean array.
   */
  public InvalidRowException(String message, boolean[] columns) {
    super(message);
    this.message = message;
    this.columns = columns;
  }

  /**
   * sets the bad row.
   * 
   * @param row is the bad line whose data description doesn't match with the specified description
   *        during the start of the system.
   */
  public void setBadRow(MutableCharArrayString row) {
    this.row = row;
  }

  /**
   * 
   * @return a MutableCharArrayString containing bad row.
   */
  public MutableCharArrayString getBadRow() {
    return this.row;
  }

  /**
   * retrieves an array of boolean. true means column has invalid data.
   * 
   * @return an array of boolean.
   */
  public boolean[] getColumns() {
    return columns;
  }

  /**
   * sets columns.
   * 
   * @param columns is a boolean array representing the column status of a row.
   */
  public void setColumns(boolean[] columns) {
    this.columns = columns;
  }

  /**
   * create a new InvalidRowException with the specified throwable.
   * 
   * @param cause specifies what is root cause for this exception.
   */
  public InvalidRowException(Throwable cause) {
    super(cause);
  }

  @Override
  public String toString() {
    return message + " [" + this.row.toString() + "] having invalid columns : "
        + Arrays.toString(columns).toString();
  }

  @Override
  public String getMessage() {
    return message + " [" + this.row.toString() + "] having invalid columns : "
        + Arrays.toString(columns).toString();
  }
}
