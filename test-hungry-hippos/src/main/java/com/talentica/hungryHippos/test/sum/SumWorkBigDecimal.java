/**
 * 
 */
package com.talentica.hungryHippos.test.sum;

import java.math.BigDecimal;
import java.math.MathContext;

import com.talentica.hungryHippos.client.domain.ExecutionContext;
import com.talentica.hungryHippos.client.domain.Work;

public class SumWorkBigDecimal implements Work {

  protected int[] dimensions;
  protected int valueIndex;
  private BigDecimal sum;
  private int jobId;
  private MathContext mc = new MathContext(64);

  public SumWorkBigDecimal(int[] dimensions, int valueIndex, int jobId) {
    this.dimensions = dimensions;
    this.valueIndex = valueIndex;
    this.jobId = jobId;
  }

  @Override
  public void processRow(ExecutionContext executionContext) {
    Object obj = executionContext.getValue(valueIndex);
    if (sum == null) {
      if (obj instanceof Integer) {
        sum = BigDecimal.valueOf((Integer) obj);
      } else if (obj instanceof Long) {
        sum = BigDecimal.valueOf((Long) obj);
      } else if (obj instanceof Double) {
        sum = BigDecimal.valueOf((Double) obj);
      } else if (obj instanceof Float) {
        sum = BigDecimal.valueOf((Float) obj);
      }
    } else {
      if (obj instanceof Integer) {
        sum = sum.add(BigDecimal.valueOf((Integer) obj), mc);
      } else if (obj instanceof Long) {
        sum = sum.add(BigDecimal.valueOf((Long) obj), mc);
      } else if (obj instanceof Double) {
        sum = sum.add(BigDecimal.valueOf((Double) obj), mc);
      } else if (obj instanceof Float) {
        sum = sum.add(BigDecimal.valueOf((Float) obj), mc);
      }
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
