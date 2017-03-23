/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
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
package com.talentica.hungryHippos.utility;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

public class HHFStream {

  private Chunk chunk;

  private long actualSizeTobeRead = 0;
  private long currentPos = 0l;

  private FileInputStream fis = null;
  private BufferedInputStream bis = null;



  public Chunk getChunk() {
    return chunk;
  }


  public long getActualSizeRead() {
    return this.actualSizeTobeRead;
  }


  public HHFStream(Chunk chunk) {
    this.chunk = chunk;
  }
  /*
   * private byte[] readLine() throws IOException {
   * 
   * if (raf == null) { raf = new RandomAccessFile(new File(chunk.getParentFilePath()), "r");
   * currentPos = chunk.getStart(); actualSizeTobeRead = chunk.getActualSizeOfChunk(); }
   * 
   * raf.seek(currentPos); if (actualSizeTobeRead > 0) { if (actualSizeTobeRead < buffer.length) {
   * 
   * System.out.println("actualSizeTobeRead is less than bufferlength "); buffer = new byte[(int)
   * actualSizeTobeRead]; } int length = raf.read(buffer); int k = 0;
   * 
   * for (int i = buffer.length - 1;; i--) { if (buffer[i] == eof[0]) { break; } buffer[i] = 0; k++;
   * } length -= k; currentPos += length; actualSizeTobeRead -= length;
   * 
   * 
   * } else if (actualSizeTobeRead == 0) { return null; }
   * 
   * 
   * return buffer; }
   */

  public int read(byte[] buffer) throws IOException {
    return read(buffer, 0, buffer.length);
  }

  public int read(byte[] buffer, int start, int end) throws IOException {

    if (fis == null) {

      fis = new FileInputStream(new File(chunk.getParentFilePath()));
      bis = new BufferedInputStream(fis);
      currentPos = chunk.getStart();
      actualSizeTobeRead = chunk.getActualSizeOfChunk();
      bis.skip(currentPos);

    }



    int bytesRead = 0;
    if (actualSizeTobeRead > 0) {
      if (actualSizeTobeRead < buffer.length) {

        bytesRead = bis.read(buffer, start, (int) actualSizeTobeRead);
      } else {
        bytesRead = bis.read(buffer, start, end);
      }
      currentPos += bytesRead;
      actualSizeTobeRead -= bytesRead;

    } else if (actualSizeTobeRead == 0) {
      return -1;
    }

    return bytesRead;
  }


  /*
   * public Stream<byte[]> lines() { Iterator<byte[]> iter = new Iterator<byte[]>() { byte[] lines =
   * null;
   * 
   * @Override public boolean hasNext() { if (lines != null) { return true; } else { try { lines =
   * readLine(); return (lines != null); } catch (IOException e) { throw new
   * UncheckedIOException(e); } } }
   * 
   * @Override public byte[] next() { if (lines != null || hasNext()) { byte[] line = lines; lines =
   * null; return line; } else { throw new NoSuchElementException(); } } }; return
   * StreamSupport.stream( Spliterators.spliteratorUnknownSize(iter, Spliterator.ORDERED |
   * Spliterator.NONNULL), false); }
   */
  public void close() {
    try {
      if (bis != null) {
        bis.close();
      }

    } catch (IOException e) {
      System.out.println(e.getMessage());
    }
  }

}
