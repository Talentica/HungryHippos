package com.talentica.hungryHippos.test.sum;

import java.io.Serializable;
import java.util.Arrays;

import com.talentica.hungryHippos.client.domain.Work;
import com.talentica.hungryHippos.client.job.Job;

/**
 * Created by debasishc on 9/9/15.
 */
public class SumJob implements Job, Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = -4111336293020419218L;
  protected int[] dimensions;
  protected int valueIndex;
  protected int jobId;

  public SumJob() {}

  public SumJob(int[] dimensions, int valueIndex, int jobId) {
    this.dimensions = dimensions;
    this.valueIndex = valueIndex;
    this.jobId = jobId;
  }


  @Override
  public Work createNewWork() {
    return new SumWork(dimensions, valueIndex, jobId);
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
      return "\nSumJob{{dimensions:" + Arrays.toString(dimensions) + ", valueIndex:" + valueIndex
          + ", job id:" + jobId + "}}";
    }
    return super.toString();
  }

  public int getIndex() {
    return valueIndex;
  }

  @Override
  public int getJobId() {
    return jobId;
  }

}
