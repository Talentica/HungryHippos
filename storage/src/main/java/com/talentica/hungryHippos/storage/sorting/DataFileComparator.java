/**
 * 
 */
package com.talentica.hungryHippos.storage.sorting;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataLocator;

/**
 * @author pooshans
 *
 */
public class DataFileComparator {

  private byte[] chunk;
  private int[] dimensions;
  
  private DataDescription dataDescription;
  public DataFileComparator(DataDescription dataDescription){
    this.dataDescription = dataDescription;
  }
  
  public void setChunk(byte[] chunk){
    this.chunk = chunk;
  }
  
  public void setDimensions(int[] dimensions){
    this.dimensions = dimensions;
  }
  
  private int columnPos1 = 0;
  private int columnPos2 = 0;
  
  public int compare(int row1Index,int row2Index){
    int res = 0;
    int rowSize = dataDescription.getSize();
    for (int dim = 0; dim < dimensions.length; dim++) {
      DataLocator locator = dataDescription.locateField(dim);
      columnPos1 = row1Index * rowSize + locator.getOffset();
      columnPos2 = row2Index * rowSize + locator.getOffset();
      for(int pointer = 0 ; pointer < locator.getSize() ; pointer++ ){
        if(chunk[columnPos1] != chunk[columnPos2]) {
          return chunk[columnPos1] - chunk[columnPos2];
        }
        columnPos1++;
        columnPos2++;
      }
    }
    return res;
  }
  
}
