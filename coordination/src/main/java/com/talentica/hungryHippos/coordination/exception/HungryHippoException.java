package com.talentica.hungryHippos.coordination.exception;

public class HungryHippoException extends Exception {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public HungryHippoException(String msg) {
    super(msg);
  }

  public HungryHippoException(String msg, Throwable cause) {
    super(msg, cause);
  }

  @Override
  public String toString() {
    return getMessage();
  }

}
