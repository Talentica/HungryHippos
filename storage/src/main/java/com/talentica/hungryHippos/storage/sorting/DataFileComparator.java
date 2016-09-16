/**
 * 
 */
package com.talentica.hungryHippos.storage.sorting;

import java.nio.ByteBuffer;
import java.util.Comparator;

import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;

/**
 * @author pooshans
 *
 */
public class DataFileComparator implements Comparator<byte[]> {

  private int[] dimenstion;

  private DynamicMarshal dynamicMarshal;
  private ByteBuffer rowBuffer1 = null;
  private ByteBuffer rowBuffer2 = null;
  private MutableCharArrayString temp1 = null;
  private MutableCharArrayString temp2 = null;
  private int res;
  public DataFileComparator(DynamicMarshal dynamicMarshal,int bufferSize) {
    this.dynamicMarshal = dynamicMarshal;
    rowBuffer1 = ByteBuffer.allocate(bufferSize);
    rowBuffer2 = ByteBuffer.allocate(bufferSize);
  }

  public int[] getDimenstion() {
    return dimenstion;
  }

  public void setDimenstion(int[] dimenstion) {
    this.dimenstion = dimenstion;
  }


  @Override
  public int compare(byte[] row1, byte[] row2) {
    rowBuffer1.clear();
    rowBuffer2.clear();
    rowBuffer1.put(row1);
    rowBuffer2.put(row2);
    for (int dim = 0; dim < dimenstion.length; dim++) {
      res = 0;
      temp1 = ((MutableCharArrayString) dynamicMarshal.readValue(dimenstion[dim], rowBuffer1)).clone();
      temp2 = (MutableCharArrayString) dynamicMarshal.readValue(dimenstion[dim], rowBuffer2);
      res = temp1.compareTo(temp2);
      if (res != 0) {
       reset();
        return res;
      }
    }
    reset();
    return res;
  }
  
  private void reset(){
    rowBuffer1.flip();
    rowBuffer2.flip();
  }
}
