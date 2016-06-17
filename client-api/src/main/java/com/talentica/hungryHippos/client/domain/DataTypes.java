package com.talentica.hungryHippos.client.domain;

import java.io.Serializable;

/**
 * This interface is written so that the client can create own class and do the processing.
 * 
 * @author sudarshans
 *
 */
public interface DataTypes extends Comparable<DataTypes>, Cloneable, Serializable {

  /**
   * for getting the length of unferlying array.
   * 
   * @return int
   */
  int getLength();

  /**
   * for getting byte at a particular index.
   * 
   * @param index
   * @return byte
   */
  byte byteAt(int index);

  /**
   * for getting the array object
   * 
   * @return
   */
  byte[] getUnderlyingArray();

  /**
   * for adding a byte in the array.
   * 
   * @param ch
   * @return
   */
  DataTypes addByte(byte ch);

  /**
   * for reseting the array length.
   */
  void reset();

  DataTypes clone();

}
