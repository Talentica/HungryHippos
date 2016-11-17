/**
 * 
 */
package com.talentica.hungryHippos.rdd.job;

import java.io.Serializable;

/**
 * @author pooshans
 *
 */
public class Job implements Serializable {
  private static final long serialVersionUID = 1L;
  private Integer[] dimensions;
  private int calculationIndex;

  public Job(Integer[] dimensions, int calculationIndex) {
    this.dimensions = dimensions;
    this.calculationIndex = calculationIndex;
  }

  public Integer[] getDimensions() {
    return dimensions;
  }

  public void setDimensions(Integer[] dimensions) {
    this.dimensions = dimensions;
  }

  public int getCalculationIndex() {
    return calculationIndex;
  }

  public void setCalculationIndex(int calculationIndex) {
    this.calculationIndex = calculationIndex;
  }



}
