package com.talentica.hungryHippos.sharding;

/**
 * {@code NodeOverflowException } , thrown when ever the Node capacity was reaches more than specified limit.
 *  @author debasishc 
 *  @since 14/8/15.
 */
public class NodeOverflowException extends Exception {

  private static final long serialVersionUID = 408695684941253328L;

  public NodeOverflowException(String message) {
    super(message);
  }
}
