/**
 * 
 */
package com.talentica.hungryHippos.client.config;

import java.util.Arrays;

public class Job {

  private static final long serialVersionUID = -4111336293020419218L;
  protected int[] dimensions;
  protected int valueIndex;
  protected int jobId;

  public Job(int[] dimensions, int valueIndex, int jobId) {
    this.dimensions = dimensions;
    this.valueIndex = valueIndex;
    this.jobId = jobId;
  }

  public int[] getDimensions() {
    return dimensions;
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

  public int getJobId() {
    return jobId;
  }

}
