package com.talentica.hungryHippos.client.data.parser.context;

import com.talentica.hungryHippos.client.domain.DataTypes;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;


/**
 * A Context represents a partially parsed CSV file in memory. It can build tokens by pushing
 * characters. It can build rows by pushing tokens. It can build a list of rows by pushing rows.
 */
public class Context {

  public static final char[] WINDOWS_LINE_SEPARATOR_CHARS = {13, 10};

  private DataTypes[] buffer;

  public Context(DataTypes[] buffer) {
    this.buffer = buffer;
  }

  private int fieldIndex = 0;

  /**
   * Latest token being built.
   */
  private String mTokenBuffer = "";

  /**
   * Keep track of trailing spaces and tabs until they can be discarded or appended to the token as
   * necessary.
   */
  private String mSpaceTrailBuffer = "";

  /**
   * Append the a letter to the latest token being built.
   *
   * @param letter to append.
   */
  public void pushTokenChar(final char letter) {
    mTokenBuffer += letter;
  }

  public void pushToken() {
    for (char tokenChar : mTokenBuffer.toCharArray()) {
      buffer[fieldIndex].addByte((byte)tokenChar);
    }
    clearTokenBuffer();
    fieldIndex++;
  }

  public void clearTokenBuffer() {
    mTokenBuffer = "";
  }

  /**
   * Add space or tab to the trailing space buffer. These space may or may not be part of the token.
   *
   * @param space or tab to push to the trailing space buffer.
   */
  public void pushSpace(final char space) {
    mSpaceTrailBuffer += space;
  }

  /**
   * The trailing space buffer ought to be pushed to the token.
   */
  public void pushSpaceTrail() {
    mTokenBuffer += mSpaceTrailBuffer;
    clearSpaceTrail();
  }

  /**
   * Discard the trailing space buffer by clearing it.
   */
  public void clearSpaceTrail() {
    mSpaceTrailBuffer = "";
  }

  /**
   * Gets the token being built.
   *
   * @return token being built.
   */
  public String getTokenBuffer() {
    return mTokenBuffer;
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

}
