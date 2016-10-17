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

  private int valueIndex = -1;
  private int jobId;
  public SumJobLocal() {}

  public SumJobLocal(int[] dimensions, int valueIndex,int jobId) {
    this.dimensions = dimensions;
    this.valueIndex = valueIndex;
    this.jobId = jobId;
  }


  @Override
  public Work createNewWork() {
    return new SumWorkLocal(dimensions, valueIndex,jobId);
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
      return "\nSumJob{dimensions:" + Arrays.toString(dimensions) + "valueIndex:" + this.valueIndex
          + "}";
    }
    return super.toString();
  }

  @Override
  public int getJobId() {
    // TODO Auto-generated method stub
    return 0;
  }

}
