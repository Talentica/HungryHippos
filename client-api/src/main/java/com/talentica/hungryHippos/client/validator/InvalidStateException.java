/**
 * 
 */
package com.talentica.hungryHippos.client.validator;


/**
 * @author pooshans
 *
 */
public class InvalidStateException extends RuntimeException {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private String message;

  public InvalidStateException(String message) {
    super(message);
    this.message = message;
  }

  public InvalidStateException(Throwable cause) {
    super(cause);
  }

  @Override
  public String toString() {
    return "InvalidStateException [message=" + message + "]";
  }

}
