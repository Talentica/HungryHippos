package com.talentica.hungryHippos.test.median.lng;

import java.io.Serializable;

import com.talentica.hungryHippos.client.domain.ExecutionContext;
import com.talentica.hungryHippos.client.domain.Work;
import com.talentica.hungryHippos.test.median.MedianCalculator;

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
  private MedianCalculator medianCalculator = new MedianCalculator();

  public MedianWork(int[] dimensions, int valueIndex) {
    this.dimensions = dimensions;
    this.valueIndex = valueIndex;
  }

  @Override
  public void processRow(ExecutionContext executionContext) {
    medianCalculator.addValue((Long) executionContext.getValue(valueIndex));
  }

  @Override
  public void calculate(ExecutionContext executionContext) {
    executionContext.saveValue(valueIndex, medianCalculator.calculate(), "Median");
  }

  @Override
  public void reset() {
    medianCalculator.clear();
  }

}
