/**
 * 
 */
package com.talentica.hungryHippos.storage.sorting;

/**
 * @author pooshans
 *
 */
public class DataFileHeapSort {
  private byte[] chunk;
  private int rowSize;
  private byte[] tmpRow;
  private DataFileComparator comparator;

  public DataFileHeapSort(int rowSize,DataFileComparator comparator) {
    this.rowSize = rowSize;
    this.comparator = comparator;
    this.tmpRow = new byte[rowSize];
  }

  public void setChunk(byte[] chunk) {
    this.chunk = chunk;
    this.comparator.setChunk(chunk);
  }

  public void heapSort() {
    int totalElement = chunk.length / rowSize;
    buildHeap(chunk, totalElement);
    for (int i = (totalElement - 1); i > 0; i--) {
      byte[] temp = readChunkRowAt(0, tmpRow);
      copyChunkRowAt((i), (0));
      copyToChunkAt(temp, (i));
      siftDown(chunk, 1, i);
    }
  }

  private byte[] readChunkRowAt(int index, byte[] row) {
    int position = index * rowSize;
    System.arraycopy(chunk, position, row, 0, rowSize);
    return row;
  }

  private void copyToChunkAt(byte[] fromRowChunk, int index) {
    int position = index * rowSize;
    System.arraycopy(fromRowChunk, 0, chunk, position, rowSize);
  }

  private void copyChunkRowAt(int fromIndex, int toindex) {
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
    for (int index = 1; index <= heapSize; index++) {
      shiftUp(index);
    }
  }

  private void shiftUp(int nodeIndex) {
    int parentIndex;
    if (nodeIndex != 1) {
      parentIndex = getParentIndex(nodeIndex);
      if (comparator.compare(parentIndex - 1, nodeIndex - 1) < 0) {
        byte[] temp = readChunkRowAt(parentIndex - 1, tmpRow);
        copyChunkRowAt((nodeIndex - 1), (parentIndex - 1));
        copyToChunkAt(temp, (nodeIndex - 1));
        shiftUp(parentIndex);
      }
    }
  }

  private int getParentIndex(int nodeIndex) {
    return (nodeIndex) / 2;
  }


  private void siftDown(byte[] chunk, int index, int heapSize) {
    int leftChild = index * 2;
    int rightChild = leftChild + 1;
    int largest;
    if (leftChild <= heapSize && comparator.compare(leftChild - 1, index - 1) > 0) {
      largest = leftChild;
    } else {
      largest = index;
    }
    if (rightChild <= heapSize && comparator.compare(rightChild - 1, largest - 1) > 0) {
      largest = rightChild;
    }
    if (largest != index) {
      byte[] temp = readChunkRowAt(index - 1, tmpRow);
      copyChunkRowAt((largest - 1), (index - 1));
      copyToChunkAt(temp, (largest - 1));
      siftDown(chunk, largest, heapSize);
    }
  }
}
