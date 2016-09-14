/**
 * 
 */
package com.talentica.hungryHippos.storage.sorting;

import java.nio.ByteBuffer;
import java.util.Comparator;

import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;

/**
 * @author pooshans
 *
 */
public class DimensionComparator {

  private int[] dimenstion;

  private DynamicMarshal dynamicMarshal;
  private ByteBuffer rowBuffer1;
  private ByteBuffer rowBuffer2;
  private StringBuilder temp1 = new StringBuilder();
  private StringBuilder temp2 = new StringBuilder();

  public DimensionComparator(DynamicMarshal dynamicMarshal) {
    this.dynamicMarshal = dynamicMarshal;
  }

  public int[] getDimenstion() {
    return dimenstion;
  }

  public void setDimenstion(int[] dimenstion) {
    this.dimenstion = dimenstion;
  }


  private Comparator<byte[]> defaultcomparator = new Comparator<byte[]>() {
    @Override
    public int compare(byte[] row1, byte[] row2) {
      rowBuffer1 = ByteBuffer.wrap(row1);
      rowBuffer2 = ByteBuffer.wrap(row2);
      int res = 0;
      for (int dim = 0; dim < dimenstion.length; dim++) {
        temp1.append(dynamicMarshal.readValue(dimenstion[dim], rowBuffer1));
        temp2.append(dynamicMarshal.readValue(dimenstion[dim], rowBuffer2));
        res = temp1.toString().compareTo(temp2.toString());
        if (res != 0) {
          reset();
          return res;
        }
      }
      reset();
      return res;
    }

    private void reset() {
      rowBuffer1.clear();
      rowBuffer2.clear();
      temp1.delete(0, temp1.length());
      temp2.delete(0, temp2.length());
    }
  };

  public Comparator<byte[]> getDefaultComparator() {
    return defaultcomparator;
  }

}
