/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
/**
 * 
 */
package com.talentica.hungryhippos.filesystem.helper;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code BinaryFileBuffer} reads binary data line by line from a file.
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
   * creates an instance of BinaryFileBuffer.
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
    reload();
  }

  /**
   * close the stream.
   * 
   * @throws IOException
   */
  public void close() throws IOException {
    this.dis.close();
  }

  /**
   * check the buffer is empty.
   * 
   * @return
   */
  public boolean empty() {
    return !isRemaining;
  }

  /**
   * show the first line.
   * 
   * @return
   */
  public ByteBuffer peek() {
    return this.readByteBuffer;
  }

  /**
   * remove the line from the buffer.
   * 
   * @return
   * @throws IOException
   */
  public ByteBuffer pop() throws IOException {
    ByteBuffer answer = copyRow(peek());
    reload();
    return answer;
  }

  private ByteBuffer copyRow(ByteBuffer answer) {
    for (int i = 0; i < answer.array().length; i++) {
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

  /**
   * retrieve the DataInputStream used.
   * 
   * @return
   */
  public DataInputStream getReader() {
    return dis;
  }


}
