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

  public DataFileHeapSort(int rowSize, DynamicMarshal dynamicMarshal) {
    this.rowSize = rowSize;
    this.comparator = new DataFileComparator(dynamicMarshal, rowSize);
  }

  public void setChunk(byte[] chunk) {
    this.chunk = chunk;
  }

  public void heapSort() {
    int totalElement = chunk.length / rowSize;
    buildHeap(chunk, totalElement);
    for (int i = (totalElement - 1); i > 0; i--) {
      byte[] temp = readRowChunkAt(0);
      copyRowChunk(readRowChunkAt(i), readRowChunkAt(0));
      copyRowChunk(readRowChunkAt(i), temp);
      doHeap(chunk, 1, i);
    }
  }

  private byte[] readRowChunkAt(int position) {
    byte[] row = new byte[rowSize];
    for (int i = position; i < rowSize; i++) {
      row[i - position] = chunk[i];
    }
    return row;
  }

  private void copyRowChunk(byte[] fromRowChunk, byte[] toRowChunk) {
    for (int i = 0; i < rowSize; i++) {
      toRowChunk[i] = fromRowChunk[i];
    }
  }

  private void buildHeap(byte[] array, int heapSize) {
    if (array == null) {
      throw new NullPointerException("null");
    }
    if (array.length <= 0 || heapSize <= 0) {
      throw new IllegalArgumentException("illegal");
    }
    if (heapSize > array.length) {
      heapSize = array.length;
    }

    for (int i = heapSize / 2; i > 0; i--) {
      doHeap(array, i, heapSize);
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
      copyRowChunk(readRowChunkAt(largest - 1), readRowChunkAt(index - 1));
      copyRowChunk(temp, readRowChunkAt(largest - 1));
      doHeap(chunk, largest, heapSize);
    }
  }
}
