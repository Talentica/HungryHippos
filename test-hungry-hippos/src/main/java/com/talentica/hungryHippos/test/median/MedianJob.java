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
  protected int jobId;
  public MedianJob() {}

  public MedianJob(int[] dimensions, int valueIndex,int jobid) {
    this.dimensions = dimensions;
    this.valueIndex = valueIndex;
    this.jobId = jobid;
  }


  @Override
  public Work createNewWork() {
    return new MedianWork(dimensions, valueIndex,jobId);
  }

  @Override
  public int[] getDimensions() {
    return dimensions;
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

  @Override
  public int getJobId() {
   return jobId;
  }

}
