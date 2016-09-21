/**
 * 
 */
package com.talentica.hungryHippos.storage.sorting;

/**
 * @author pooshans
 *
 */
public class InsufficientMemoryException extends Exception {

  private static final long serialVersionUID = 1L;
  private String message;

  public InsufficientMemoryException(String message) {
    super(message);
    this.message = message;
  }

  public String getMessage() {
    return message;
  }

}
