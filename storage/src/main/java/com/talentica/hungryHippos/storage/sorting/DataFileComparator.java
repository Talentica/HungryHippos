/**
 * 
 */
package com.talentica.hungryHippos.storage.sorting;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataLocator;

/**
 * This is the class to compare the row for sorting for particular file chunk in binary format.
 * 
 * @author pooshans
 *
 */
public class DataFileComparator {

  private byte[] chunk;
  private int[] dimensions;

  private DataDescription dataDescription;

  /**
   * Parameterized constructor
   * 
   * @param dataDescription
   */
  public DataFileComparator(DataDescription dataDescription) {
    this.dataDescription = dataDescription;
  }

  /**
   * Set the chunk
   * 
   * @param chunk
   */
  public void setChunk(byte[] chunk) {
    this.chunk = chunk;
  }

  /**
   * Set dimensions to compare.
   * 
   * @param dimensions
   */
  public void setDimensions(int[] dimensions) {
    this.dimensions = dimensions;
  }

  private int columnPos1 = 0;
  private int columnPos2 = 0;

  /**
   * @param row1Index
   * @param row2Index
   * @return negative integer value if row1 < row2 , zero if row1 = row2, and positive integer value
   *         if row1 > row2
   */
  public int compare(int row1Index, int row2Index) {
    int res = 0;
    int rowSize = dataDescription.getSize();
    for (int dim = 0; dim < dimensions.length; dim++) {
      DataLocator locator = dataDescription.locateField(dimensions[dim]);
      columnPos1 = row1Index * rowSize + locator.getOffset();
      columnPos2 = row2Index * rowSize + locator.getOffset();
      for (int pointer = 0; pointer < locator.getSize(); pointer++) {
        if (chunk[columnPos1] != chunk[columnPos2]) {
          return chunk[columnPos1] - chunk[columnPos2];
        }
        columnPos1++;
        columnPos2++;
      }
    }
    return res;
  }

}
