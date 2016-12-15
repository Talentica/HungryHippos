/**
 * 
 */
package com.talentica.hungryHippos.rdd;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;

import scala.collection.AbstractIterator;

/**
 * @author pooshans
 *
 */
public class HHRDDIterator extends AbstractIterator<HHRDDRowReader> implements Serializable {

  private static final long serialVersionUID = 6639856882717975103L;
  private ByteBuffer byteBuffer = null;
  private byte[] byteBufferBytes;
  private long currentDataFileSize;
  private BufferedInputStream dataInputStream;
  private HHRDDRowReader hhRDDRowReader;
  private int recordLength;

  public HHRDDIterator(String filePath,int rowSize,FieldTypeArrayDataDescription dataDescription) throws IOException {

    this.dataInputStream = new BufferedInputStream( new FileInputStream(filePath),2097152);
    this.currentDataFileSize = dataInputStream.available();
    this.hhRDDRowReader = new HHRDDRowReader(dataDescription);
    this.byteBufferBytes = new byte[rowSize];
    this.byteBuffer = ByteBuffer.wrap(byteBufferBytes);
    this.recordLength = rowSize;
    this.hhRDDRowReader.setByteBuffer(byteBuffer);
  }

  @Override
  public boolean hasNext() {
    try {
      if (currentDataFileSize <= 0) {
        closeDatsInputStream();
      }
    } catch (IOException exception) {
      throw new RuntimeException(exception);
    }
    return currentDataFileSize > 0;

  }

  @Override
  public HHRDDRowReader next() {
    try {
      dataInputStream.read(byteBufferBytes);
      currentDataFileSize = currentDataFileSize - recordLength;      
    } catch (IOException e) {
      e.printStackTrace();
    }
    return hhRDDRowReader;
  }

  private void closeDatsInputStream() throws IOException {
    if (dataInputStream != null) {
      dataInputStream.close();
    }
  }
  
 /* public static void main(String args[]) {
    byte[] byteArr = new byte[4];
    ByteBuffer byteBuffer =  ByteBuffer.wrap(byteArr);
    
    byte[] intdata =  ByteBuffer.allocate(4).putInt(200).array();
    System.arraycopy(intdata, 0, byteArr, 0, 4);
    byte[] intdata2 =  ByteBuffer.allocate(4).putInt(19989).array();
    System.arraycopy(intdata2, 0, byteArr, 0, 4);
    System.out.println(byteBuffer.getInt());
  }*/

}
