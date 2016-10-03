package com.talentica.hungryHippos.test.sum.local;

import java.io.Serializable;
import java.util.Arrays;

import com.talentica.hungryHippos.client.domain.Work;
import com.talentica.hungryHippos.client.job.Job;

/**
 * Created by debasishc on 9/9/15.
 */
public class SumJobLocal implements Job, Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = -4111336293020419218L;
  protected int[] dimensions;

  public SumJobLocal() {}

  public SumJobLocal(int[] dimensions, int primaryDimension) {
    this.dimensions = dimensions;
  }


  @Override
  public Work createNewWork() {
    return new SumWorkLocal(dimensions);
  }

  @Override
  public int[] getDimensions() {
    return dimensions;
  }

  public long getMemoryFootprint(long rowCount) {
    return 8;
  }

  @Override
  public String toString() {
    if (dimensions != null) {
      return "\nSumJob{{dimensions:" + Arrays.toString(dimensions) + ", valueIndex:3 & 6" + "}}";
    }
    return super.toString();
  }

}
