/**
 * 
 */
package com.talentica.hungryHippos.storage.sorting;

import java.io.DataInputStream;
import java.io.EOFException;
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
  private ByteBuffer cache;
  byte[] bytes;
  boolean isRemaining = false;

  public BinaryFileBuffer(DataInputStream dis, int bufferSize) throws IOException {
    this.dis = dis;
    bytes = new byte[bufferSize];
    cache = ByteBuffer.wrap(bytes);
    reload();
  }

  public void close() throws IOException {
    this.dis.close();
  }

  public boolean empty() {
    return !isRemaining;
  }

  public ByteBuffer peek() {
    return this.cache;
  }

  public ByteBuffer pop() throws IOException {
    ByteBuffer answer = peek();
    reload();
    return answer;
  }

  private void reload() throws IOException {
    cache.clear();
    read();
  }

  private void read() throws IOException {
    try {
      this.dis.readFully(bytes);
      isRemaining = true;
    } catch (EOFException eof) {
      isRemaining = false;
    }
  }

  public DataInputStream getReader() {
    return dis;
  }


}
