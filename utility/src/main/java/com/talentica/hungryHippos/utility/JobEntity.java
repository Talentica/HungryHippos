/**
 * 
 */
package com.talentica.hungryHippos.utility;

import java.io.Serializable;

import org.apache.commons.lang3.builder.ToStringBuilder;

import com.talentica.hungryHippos.client.job.Job;

/**
 * @author PooshanS
 *
 */
public class JobEntity implements Serializable {
  private static final long serialVersionUID = 7062343992924390450L;
  private int jobId;
  private static int counter;
  private Job job;
  private String status;

  private int[] dimensionsPointer;
  
  
  public JobEntity(Job job) {
    this();
    this.job = job;
  }

  public JobEntity() {
    jobId = counter++;
  }

  public int getJobId() {
    return jobId;
  }

  public Job getJob() {
    return job;
  }

  public void setJob(Job job) {
    this.job = job;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  @Override
  public boolean equals(Object object) {
    boolean result = false;
    if (object == null || object.getClass() != getClass()) {
      result = false;
    } else {
      JobEntity jobEntity = (JobEntity) object;
      if (this.jobId == jobEntity.getJobId()) {
        result = true;
      }
    }
    return result;
  }

  @Override
  public int hashCode() {
    return this.jobId;
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this);
  }

  public int[] getDimensionsPointer() {
    return dimensionsPointer;
  }

  public void setDimensionsPointer(int[] dimensionsPointer) {
    this.dimensionsPointer = dimensionsPointer;
  }
  
  

}
