package com.talentica.hungryhippos.filesystem.Exception;

import org.apache.zookeeper.KeeperException;

/**
 * Exception Class used on FileSystem.
 * 
 * @author sudarshans
 *
 */
public class HungryHipposFileSystemException extends KeeperException {

  public HungryHipposFileSystemException(Code code) {
    super(code);
  }

  /**
   * Serial UID for the serialization
   */
  private static final long serialVersionUID = 2529936268877346952L;
}
