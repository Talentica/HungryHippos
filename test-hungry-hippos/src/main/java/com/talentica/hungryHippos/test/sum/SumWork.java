package com.talentica.hungryHippos.test.sum;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.MathContext;

import com.talentica.hungryHippos.client.domain.ExecutionContext;
import com.talentica.hungryHippos.client.domain.Work;

public class SumWork implements Work, Serializable {

  private static final long serialVersionUID = -5931349264723731947L;
  protected int[] dimensions;
  protected int valueIndex;
  private BigDecimal sum;
  private int jobId;
  private MathContext mc = new MathContext(64);

  public SumWork(int[] dimensions, int valueIndex, int jobId) {
    this.dimensions = dimensions;
    this.valueIndex = valueIndex;
    this.jobId = jobId;
  }

  @Override
  public void processRow(ExecutionContext executionContext) {
    if (sum == null) {
      sum = BigDecimal.valueOf(Double.valueOf(executionContext.getValue(valueIndex).toString()));
    } else {
      sum = sum.add(
          BigDecimal.valueOf(Double.valueOf(executionContext.getValue(valueIndex).toString())), mc);
    }
  }

  @Override
  public void calculate(ExecutionContext executionContext) {
    executionContext.saveValue(jobId, valueIndex, sum, "Sum");
  }

  @Override
  public void reset() {
    sum = null;
  }

  @Override
  public int getJobId() {
    return jobId;
  }

}
