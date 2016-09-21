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
    reload();
  }

  public void close() throws IOException {
    this.dis.close();
  }

  public boolean empty() {
    return !isRemaining;
  }

  public ByteBuffer peek() {
    return this.readByteBuffer;
  }

  public ByteBuffer pop() throws IOException {
    ByteBuffer answer = copyRow(peek());
    reload();
    return answer;
  }

  private ByteBuffer copyRow(ByteBuffer answer) {
    for(int i  =0; i < answer.array().length ; i++){
      lastByteRead[i] = answer.get(i);
    }
    return lastRowByteBuffer;
  }
  
  private void reload() throws IOException {
    readByteBuffer.clear();
    read();
  }

  private void read() throws IOException {
    try {
      this.dis.readFully(readBytes);
      isRemaining = true;
    } catch (EOFException eof) {
      isRemaining = false;
    }
  }

  public DataInputStream getReader() {
    return dis;
  }


}
