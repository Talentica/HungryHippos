package com.talentica.hungryHippos.coordination.exception;

/**
 * {@code HungryHippoException} is used by the application to capture exception related to the
 * system.
 * 
 * @author sudarshans.
 * 
 */
public class HungryHippoException extends Exception {

  /**
   * Default serial id.
   */
  private static final long serialVersionUID = 1L;

  /**
   * create a new instance of HungryHippoException with provided {@value msg}
   * 
   * @param msg
   */
  public HungryHippoException(String msg) {
    super(msg);
  }

  /**
   * create a new instance of HungryHippoException with provided {@value msg} and {@value cause}.
   * where cause is {@link Throwable}
   * 
   * @param msg
   * @param cause
   */
  public HungryHippoException(String msg, Throwable cause) {
    super(msg, cause);
  }

  @Override
  public String toString() {
    return getMessage();
  }

}
