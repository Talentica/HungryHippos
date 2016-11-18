/**
 * 
 */
package com.talentica.hungryHippos.rdd.job;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author pooshans
 *
 */
public class Job implements Serializable {
  private static final long serialVersionUID = 1L;
  private Integer[] dimensions;
  private int calculationIndex;
  private int jobId;

  public Job(Integer[] dimensions, int calculationIndex, int jobId) {
    this.dimensions = dimensions;
    this.calculationIndex = calculationIndex;
    this.jobId = jobId;
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

  public int getJobId() {
    return jobId;
  }

  public void setJobId(int jobId) {
    this.jobId = jobId;
  }

  @Override
  public String toString() {
    return "Job [dimensions=" + Arrays.toString(dimensions) + ", calculationIndex="
        + calculationIndex + ", jobId=" + jobId + ", getDimensions()="
        + Arrays.toString(getDimensions()) + ", getCalculationIndex()=" + getCalculationIndex()
        + ", getJobId()=" + getJobId() + ", getClass()=" + getClass() + ", hashCode()=" + hashCode()
        + ", toString()=" + super.toString() + "]";
  }



}
