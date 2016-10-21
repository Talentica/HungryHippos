/**
 * 
 */
package com.talentica.hungryHippos.storage.sorting;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * To read the binary file row by row.
 * 
 * @author pooshans
 *
 */
public final class BinaryFileBuffer {

  public static final Logger LOGGER = LoggerFactory.getLogger(BinaryFileBuffer.class);
  private DataInputStream dis;
  private ByteBuffer readByteBuffer;
  private ByteBuffer lastRowByteBuffer;
  byte[] readBytes;
  byte[] lastByteRead;
  boolean isRemaining = false;

  /**
   * Parameterized constructor
   * 
   * @param dis
   * @param bufferSize
   * @throws IOException
   */
  public BinaryFileBuffer(DataInputStream dis, int bufferSize) throws IOException {
    this.dis = dis;
    readBytes = new byte[bufferSize];
    lastByteRead = new byte[bufferSize];
    readByteBuffer = ByteBuffer.wrap(readBytes);
    lastRowByteBuffer = ByteBuffer.wrap(lastByteRead);
    isRemaining = (dis.available() > 0);
    reload();
  }

  /**
   * To close the datainputstream
   * 
   * @throws IOException
   */
  public void close() throws IOException {
    this.dis.close();
  }

  /**
   * @return true if read is available otherwise false.
   */
  public boolean empty() {
    return !isRemaining;
  }

  /**
   * To get the peek of the row. It does not remove the data from ByteBuffer.
   * 
   * @return row in ByteBuffer
   */
  public ByteBuffer peek() {
    return this.readByteBuffer;
  }

  /**
   * To get the peek row and reload the next row in the ByteBuffer.
   * 
   * @return row in ByteBuffer
   * @throws IOException
   */
  public ByteBuffer pop() throws IOException {
    lastRowByteBuffer.clear();
    System.arraycopy(peek().array(), 0, lastByteRead, 0, peek().capacity());
    reload();
    return lastRowByteBuffer;
  }

  /**
   * To reload the next row in ByteBuffer.
   * 
   * @throws IOException
   */
  private void reload() throws IOException {
    readByteBuffer.clear();
    read();
  }

  /**
   * To read the next available row from input stream to ByteBuffer.
   * 
   * @throws IOException
   */
  private void read() throws IOException {
    if (dis.available() > 0) {
      this.dis.readFully(readBytes);
    } else {
      isRemaining = false;
    }
  }

  /**
   * Get the datainputstream
   * 
   * @return
   */
  public DataInputStream getReader() {
    return dis;
  }


}
