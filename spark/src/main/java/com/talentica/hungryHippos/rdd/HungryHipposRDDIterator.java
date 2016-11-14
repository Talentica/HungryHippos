/**
 * 
 */
package com.talentica.hungryHippos.rdd;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import scala.collection.AbstractIterator;

/**
 * @author pooshans
 *
 */
public class HungryHipposRDDIterator extends AbstractIterator<ByteBuffer> {

  private ByteBuffer byteBuffer = null;
  private byte[] byteBufferBytes;
  private long currentDataFileSize;
  private DataInputStream dataInputStream;

  public HungryHipposRDDIterator(HungryHipposRDDPartition hhRDDPartion) throws IOException {
    this.dataInputStream = hhRDDPartion.getDataInputStream();
    this.currentDataFileSize = dataInputStream.available();
    this.byteBuffer = ByteBuffer.allocate(hhRDDPartion.getRowSize());
  }

  @Override
  public boolean hasNext() {
    try {
      if (currentDataFileSize <= 0) {
        closeDatsInputStream(dataInputStream);
      }
    } catch (IOException exception) {
      throw new RuntimeException(exception);
    }
    return currentDataFileSize > 0;

  }

  @Override
  public ByteBuffer next() {
    byteBuffer.clear();
    try {
      dataInputStream.readFully(byteBufferBytes);
      currentDataFileSize = currentDataFileSize - byteBufferBytes.length;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return byteBuffer;
  }

  private void closeDatsInputStream(DataInputStream in) throws IOException {
    if (in != null) {
      in.close();
    }
  }

}
