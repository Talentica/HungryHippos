package com.talentica.hungryHippos.test.median;

import java.io.Serializable;
import java.math.BigDecimal;

import com.talentica.hungryHippos.client.domain.ExecutionContext;
import com.talentica.hungryHippos.client.domain.Work;

/**
 * Created by debasishc on 9/9/15.
 */
public class MedianWork implements Work, Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = -5931349264723731947L;
  protected int[] dimensions;
  protected int valueIndex;
  private int jobId;
  private MedianCalculator medianCalculator = new MedianCalculator();

  public MedianWork(int[] dimensions, int valueIndex,int jobId) {
    this.dimensions = dimensions;
    this.valueIndex = valueIndex;
    this.jobId = jobId;
  }

  @Override
  public void processRow(ExecutionContext executionContext) {
    Object obj = executionContext.getValue(valueIndex);
    if (obj instanceof Integer) {
      medianCalculator.addValue((Integer)obj);
    } else if (obj instanceof Long) {
      medianCalculator.addValue((Long)obj);
    } else if (obj instanceof Double) {
      medianCalculator.addValue((Double)obj);
    }
  }

  @Override
  public void calculate(ExecutionContext executionContext) {
    executionContext.saveValue(jobId,valueIndex, medianCalculator.calculate(), "Median");
  }

  @Override
  public void reset() {
    medianCalculator.clear();
  }

  @Override
  public int getJobId() {
    return jobId;
  }

}
