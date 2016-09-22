/**
 * 
 */
package com.talentica.hungryHippos.storage.sorting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * @author pooshans
 *
 */
public class DataFileMergeSort<T> {
  
  private List<T> items;
  private List<T> helper;

  private Comparator<T> comparator;
  private int count;

  public void sort(T[] values, Comparator<T> comparator) {
      items = new ArrayList<T>();
      items.addAll(Arrays.asList(values));
      count = values.length;
      this.helper = new ArrayList<T>(count);
      this.comparator = comparator;
      mergesort(0, (count - 1));
  }

  public void sort(List<T> values, Comparator<T> comprtr) {
      items = values;
      count = values.size();
      this.helper = new ArrayList<T>(count);
      this.comparator = comprtr;
      mergesort(0, (count - 1));
  }   

  private void mergesort(int low, int high) {
      if (low < high) {
          int middle = low + (high - low) / 2;
          mergesort(low, middle);
          mergesort(middle + 1, high);
          merge(low, middle, high);
      }
  }

  private void merge(int low, int middle, int high) {
      for (int i = low; i <= high; i++) {
          helper.add(i, items.get(i));
      }

      int i = low;
      int j = middle + 1;
      int k = low;
      while (i <= middle && j <= high) {
          int cm = comparator.compare(helper.get(i), helper.get(j));
          if (cm <= 0) {
              items.set(k, helper.get(i));
              i++;
          } else {
              items.set(k, helper.get(j));
              j++;
          }
          k++;
      }
      while (i <= middle) {
          items.set(k, helper.get(i));
          k++;
          i++;
      }

  }


}
