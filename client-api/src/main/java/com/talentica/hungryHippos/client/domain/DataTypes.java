package com.talentica.hungryHippos.client.domain;

import java.io.Serializable;

/**
 * This interface is written so that the client can create own class and do the processing.
 * 
 * @author sudarshans
 *
 */
public interface DataType extends Cloneable, Serializable {

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
  DataType addByte(byte ch);

  /**
   * for adding a char in the array. use this method only if you want to convert the byte
   * representation to character
   * 
   * @param ch
   * @return
   */
  DataType addCharacter(char ch);

  /**
   * for reseting the array length.
   */
  void reset();

}
