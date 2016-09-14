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
public class DataFileComparator {

  private int[] dimension;

  private DynamicMarshal dynamicMarshal;
  private ByteBuffer rowBuffer1;
  private ByteBuffer rowBuffer2;
  private StringBuilder temp1 = new StringBuilder();
  private StringBuilder temp2 = new StringBuilder();

  public DataFileComparator(DynamicMarshal dynamicMarshal) {
    this.dynamicMarshal = dynamicMarshal;
  }

  public int[] getDimenstion() {
    return dimension;
  }

  public void setDimenstion(int[] dimenstion) {
    this.dimension = dimenstion;
  }

  private Comparator<byte[]> defaultDimensionComparator = new Comparator<byte[]>() {
    int res = 0;
    @Override
    public int compare(byte[] row1, byte[] row2) {
      rowBuffer1 = ByteBuffer.wrap(row1);
      rowBuffer2 = ByteBuffer.wrap(row2);
      for (int dim = 0; dim < dimension.length; dim++) {
        temp1.append(dynamicMarshal.readValue(dimension[dim], rowBuffer1));
        temp2.append(dynamicMarshal.readValue(dimension[dim], rowBuffer2));
        res = temp1.toString().compareTo(temp2.toString());
        if (res != 0) {
          break;
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
    return defaultDimensionComparator;
  }

}
