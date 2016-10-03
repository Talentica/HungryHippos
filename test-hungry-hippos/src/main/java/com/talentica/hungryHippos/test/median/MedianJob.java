package com.talentica.hungryHippos.test.median;

import java.io.Serializable;
import java.util.Arrays;

import com.talentica.hungryHippos.client.domain.Work;
import com.talentica.hungryHippos.client.job.Job;

/**
 * Created by debasishc on 9/9/15.
 */
public class MedianJob implements Job, Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = -4111336293020419218L;
  protected int[] dimensions;
  protected int valueIndex;

  public MedianJob() {}

  public MedianJob(int[] dimensions, int valueIndex) {
    this.dimensions = dimensions;
    this.valueIndex = valueIndex;
  }


  @Override
  public Work createNewWork() {
    return new MedianWork(dimensions, valueIndex);
  }

  @Override
  public int[] getDimensions() {
    return dimensions;
  }

  public long getMemoryFootprint(long rowCount) {
    return 58 * rowCount;
  }

  @Override
  public String toString() {
    if (dimensions != null) {
      return "\nMedianJob{{dimensions" + Arrays.toString(dimensions) + ", valueIndex:" + valueIndex
          + "}}";
    }
    return super.toString();
  }

  public int getIndex() {
    return valueIndex;
  }

}
