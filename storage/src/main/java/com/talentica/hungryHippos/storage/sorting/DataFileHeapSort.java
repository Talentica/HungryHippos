/**
 * 
 */
package com.talentica.hungryHippos.storage.sorting;

import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;

/**
 * @author pooshans
 *
 */
public class DataFileHeapSort {
  private byte[] chunk;
  private int rowSize;
  private DataFileComparator comparator;
  byte[] tmpRow1;
  byte[] tmpRow2;
  public DataFileHeapSort(int rowSize, DynamicMarshal dynamicMarshal, DataFileComparator comparator) {
    this.rowSize = rowSize;
    this.comparator = comparator;
    tmpRow1  = new byte[rowSize];
    tmpRow2 = new byte[rowSize];
  }

  public void setChunk(byte[] chunk) {
    this.chunk = chunk;
  }

  public void heapSort() {
    int totalElement = chunk.length / rowSize;
    buildHeap(chunk, totalElement);
    for (int i = (totalElement - 1); i > 0; i--) {
      byte[] temp = readChunkRowAt(0,tmpRow1);
      copyChunkRowAt((i), (0));
      copyToChunkAt(temp,(i));
      doHeap(chunk, 1, i);
    }
  }

  private byte[] readChunkRowAt(int index,byte[] row) {
    int position = index * rowSize;
    System.arraycopy(chunk, position, row, 0, rowSize);
    return row;
  }

  private void copyToChunkAt(byte[] fromRowChunk, int index) {
    int position = index * rowSize;
    System.arraycopy(fromRowChunk, 0, chunk, position, rowSize);
  }
  
  private void copyChunkRowAt(int fromIndex, int toindex){
    int counter = 0;
    int fromPosition = fromIndex * rowSize;
    int toPosition = toindex * rowSize;
    System.arraycopy(chunk, fromPosition, chunk, toPosition, rowSize);
  }

  private void buildHeap(byte[] chunk, int heapSize) {
    if (chunk == null) {
      throw new NullPointerException("No chunk to sort");
    }
    if (chunk.length <= 0 || heapSize <= 0) {
      throw new IllegalArgumentException("No records to sort.");
    }
    if (heapSize > (chunk.length / rowSize)) {
      heapSize = (chunk.length / rowSize);
    }

    for (int index = heapSize / 2; index > 0; index--) {
      doHeap(chunk, index, heapSize);
    }
  }

  private void doHeap(byte[] chunk, int index, int heapSize) {
    int left = index * 2;
    int right = left + 1;
    int largest;
    if (left <= heapSize
        && comparator.compare(readChunkRowAt(left - 1,tmpRow1), readChunkRowAt(index - 1,tmpRow2)) > 0) {
      largest = left;
    } else {
      largest = index;
    }
    if (right <= heapSize
        && comparator.compare(readChunkRowAt(right - 1,tmpRow1), readChunkRowAt(largest - 1,tmpRow2)) > 0) {
      largest = right;
    }
    if (largest != index) {
      byte[] temp = readChunkRowAt(index - 1,tmpRow1);
      copyChunkRowAt((largest - 1),(index - 1));
      copyToChunkAt(temp, (largest - 1));
      doHeap(chunk, largest, heapSize);
    }
  }
}
