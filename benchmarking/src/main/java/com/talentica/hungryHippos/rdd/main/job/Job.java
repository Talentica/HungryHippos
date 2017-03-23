/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
/**
 * 
 */
package com.talentica.hungryHippos.rdd.main.job;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author pooshans
 *
 */
public class Job implements Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = -6241147664348013284L;
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

