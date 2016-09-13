/**
 * 
 */
package com.talentica.hungryHippos.storage.sorting;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author pooshans
 *
 */
public final class BinaryFileBuffer {

  private DataInputStream dis;
  private ByteBuffer cache;
  byte[] bytes;

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
    return !this.cache.hasRemaining();
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
    this.dis.readFully(bytes);
  }

  public DataInputStream getReader() {
    return dis;
  }


}
