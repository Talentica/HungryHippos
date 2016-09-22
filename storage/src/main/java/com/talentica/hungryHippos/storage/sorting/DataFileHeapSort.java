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
  ;

  public <T> DataFileHeapSort(int rowSize, DynamicMarshal dynamicMarshal, DataFileComparator comparator) {
    this.rowSize = rowSize;
    this.comparator = comparator;
   
  }

  public void setChunk(byte[] chunk) {
    this.chunk = chunk;
  }

  public void heapSort() {
    int totalElement = chunk.length / rowSize;
    buildHeap(chunk, totalElement);
    for (int i = (totalElement - 1); i > 0; i--) {
      byte[] temp = readRowChunkAt(0);
      swapChunkRowAt((i), (0));
      copyToChunkAt(temp,(i));
      doHeap(chunk, 1, i);
    }
  }

  private byte[] readRowChunkAt(int position) {
    byte[] row  = new byte[rowSize];
    for (int i = position; i < (position + rowSize); i++) {
      row[i - position] = chunk[i];
    }
    return row;
  }

  private void copyToChunkAt(byte[] fromRowChunk, int position) {
    for (int i = 0; i < rowSize; i++) {
      chunk[position++] = fromRowChunk[i];
    }
  }
  
  private void swapChunkRowAt(int fromPosition, int toPosition){
    int counter = 0;
    while(counter < rowSize){
      chunk[toPosition++] = chunk[fromPosition++];
      counter++;
    }
  }

  private void buildHeap(byte[] chunk, int heapSize) {
    if (chunk == null) {
      throw new NullPointerException("null");
    }
    if (chunk.length <= 0 || heapSize <= 0) {
      throw new IllegalArgumentException("illegal");
    }
    if (heapSize > (chunk.length / rowSize)) {
      heapSize = (chunk.length / rowSize);
    }

    for (int i = heapSize / 2; i > 0; i--) {
      doHeap(chunk, i, heapSize);
    }
  }

  private void doHeap(byte[] chunk, int index, int heapSize) {
    int left = index * 2;
    int right = index * 2 + 1;
    int largest;
    if (left <= heapSize
        && comparator.compare(readRowChunkAt(left - 1), readRowChunkAt(index - 1)) > 0) {
      largest = left;
    } else {
      largest = index;
    }
    if (right <= heapSize
        && comparator.compare(readRowChunkAt(right - 1), readRowChunkAt(largest - 1)) > 0) {
      largest = right;
    }
    if (largest != index) {
      byte[] temp = readRowChunkAt(index - 1);
      swapChunkRowAt((largest - 1),(index - 1));
      copyToChunkAt(temp, (largest - 1));
      doHeap(chunk, largest, heapSize);
    }
  }
}
