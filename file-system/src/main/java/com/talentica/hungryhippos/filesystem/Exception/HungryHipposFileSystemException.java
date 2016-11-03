package com.talentica.hungryhippos.filesystem.Exception;

/**
 * Exception Class used on FileSystem.
 * 
 * @author sudarshans
 *
 */
public class HungryHipposFileSystemException extends Exception {

  private String msg = null;

  /**
   * creates instance of HungryHipposFileSystem with given message.
   * 
   * @param msg
   */
  public HungryHipposFileSystemException(String msg) {
    super(msg);
    this.msg = msg;
  }

  /**
   * creates instance of HungryHipposFileSystem with given message and cause.
   * 
   * @param msg
   * @param cause
   */
  public HungryHipposFileSystemException(String msg, Throwable cause) {
    super(msg, cause);
    this.msg = msg;
  }

  @Override
  public String toString() {
    return this.msg;
  }

  /**
   * Serial UID for the serialization
   */
  private static final long serialVersionUID = 2529936268877346952L;
}
