/**
 * 
 */
package com.talentica.hungryHippos.rdd;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;

import scala.collection.AbstractIterator;

/**
 * @author pooshans
 *
 */
public class HungryHipposRDDIterator extends AbstractIterator<HHRDDRowReader> {

  private ByteBuffer byteBuffer = null;
  private byte[] byteBufferBytes;
  private long currentDataFileSize;
  private DataInputStream dataInputStream;
  private HHRDDRowReader hhRDDRowReader;

  public HungryHipposRDDIterator(HungryHipposRDDPartition hhRDDPartion) throws IOException {

    this.dataInputStream = new DataInputStream(new FileInputStream(hhRDDPartion.getFilePath()));
    this.currentDataFileSize = dataInputStream.available();
    this.hhRDDRowReader = new HHRDDRowReader(hhRDDPartion.getDataDescription());
    this.byteBufferBytes = new byte[hhRDDPartion.getRowSize()];
    this.byteBuffer = ByteBuffer.wrap(byteBufferBytes);
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
  public HHRDDRowReader next() {
    byteBuffer.clear();
    try {
      dataInputStream.readFully(byteBufferBytes);
      currentDataFileSize = currentDataFileSize - byteBufferBytes.length;
      hhRDDRowReader.setByteBuffer(byteBuffer);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return hhRDDRowReader;
  }

  private void closeDatsInputStream(DataInputStream in) throws IOException {
    if (in != null) {
      in.close();
    }
  }

}
