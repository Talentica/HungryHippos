package com.talentica.hungryHippos.client.data.parser.context;

import com.talentica.hungryHippos.client.domain.DataTypes;


/**
 * A Context represents a partially parsed CSV file in memory. It can build tokens by pushing
 * characters. It can build rows by pushing tokens. It can build a list of rows by pushing rows.
 * 
 * @author pooshans
 */
public class Context {

  public static final char[] WINDOWS_LINE_SEPARATOR_CHARS = {13, 10};

  private DataTypes[] buffer;
  private byte[] array = new byte[Integer.BYTES];

  public Context(DataTypes[] buffer) {
    this.buffer = buffer;
  }

  private int fieldIndex = 0;

  /**
   * Latest token being built. It is reusable placeholder for token.
   */
  private StringBuilder tokenBuffer = new StringBuilder();

  /**
   * It is reusable placeholder which keep track of trailing spaces and tabs until they can be
   * discarded or appended to the token as necessary.
   */
  private StringBuilder spaceTrailBuffer = new StringBuilder();

  /**
   * Append the a letter to the latest token being built.
   *
   * @param letter to append.
   */
  public void pushTokenChar(final char letter) {
    tokenBuffer.append(letter);
  }

  public void pushToken() {
    buffer[fieldIndex].reset();
    buffer[fieldIndex].addValue(tokenBuffer);
    clearTokenBuffer();
    fieldIndex++;
  }

  public void clearTokenBuffer() {
    tokenBuffer.setLength(0);
  }

  /**
   * Add space or tab to the trailing space buffer. These space may or may not be part of the token.
   *
   * @param space or tab to push to the trailing space buffer.
   */
  public void pushSpace(final char space) {
    spaceTrailBuffer.append(space);
  }

  /**
   * The trailing space buffer ought to be pushed to the token.
   */
  public void pushSpaceTrail() {
    tokenBuffer.append(spaceTrailBuffer.toString());
    clearSpaceTrail();
  }

  /**
   * Discard the trailing space buffer by clearing it.
   */
  public void clearSpaceTrail() {
    spaceTrailBuffer.setLength(0);
  }

  /**
   * Gets the token being built.
   *
   * @return token being built.
   */
  public StringBuilder getTokenBuffer() {
    return tokenBuffer;
  }

  public DataTypes[] getParsedRow() {
    return buffer;
  }

  public void resetBuffer() {
    for (DataTypes s : buffer) {
      s.reset();
    }
    fieldIndex = 0;
  }

  public void intToByteArray(int data) {
    array[0] = (byte) ((data >> 24) & 0xff);
    array[1] = (byte) ((data >> 16) & 0xff);
    array[2] = (byte) ((data >> 8) & 0xff);
    array[3] = (byte) ((data >> 0) & 0xff);
  }

}
