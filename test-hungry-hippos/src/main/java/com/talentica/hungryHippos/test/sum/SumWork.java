package com.talentica.hungryHippos.test.sum;

import java.io.Serializable;

import com.talentica.hungryHippos.client.domain.ExecutionContext;
import com.talentica.hungryHippos.client.domain.Work;

/**
 * Created by debasishc on 9/9/15.
 */
public class SumWork implements Work, Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = -5931349264723731947L;
  protected int[] dimensions;
  protected int valueIndex;
  private double sum;
  private int jobId;

  public SumWork(int[] dimensions, int valueIndex,int jobId) {
    this.dimensions = dimensions;
    this.valueIndex = valueIndex;
    this.jobId = jobId;
  }

  @Override
  public void processRow(ExecutionContext executionContext) {
    sum = sum + ((Double) executionContext.getValue(valueIndex));
  }

  @Override
  public void calculate(ExecutionContext executionContext) {
    executionContext.saveValue(jobId,valueIndex, sum, "Sum");
  }

  @Override
  public void reset() {
    sum = 0;
  }

  @Override
  public int getJobId() {
   return jobId;
  }

}
