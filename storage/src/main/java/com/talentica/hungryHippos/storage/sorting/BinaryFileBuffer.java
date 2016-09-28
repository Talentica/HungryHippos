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

  public BinaryFileBuffer(DataInputStream dis, int bufferSize) throws IOException {
    this.dis = dis;
    readBytes = new byte[bufferSize];
    lastByteRead = new byte[bufferSize];
    readByteBuffer = ByteBuffer.wrap(readBytes);
    lastRowByteBuffer = ByteBuffer.wrap(lastByteRead);
    isRemaining = (dis.available() > 0);
    reload();
  }

  public void close() throws IOException {
    this.dis.close();
  }

  public boolean empty() throws IOException {
    return !isRemaining;
  }

  public ByteBuffer peek() {
    return this.readByteBuffer;
  }

  public void reload() throws IOException {
    readByteBuffer.clear();
    read();
  }

  private void read() throws IOException {
    if (dis.available() > 0) {
      this.dis.readFully(readBytes);
    } else {
      isRemaining = false;
    }
  }

  public DataInputStream getReader() {
    return dis;
  }
}
