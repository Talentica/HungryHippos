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
public class JobEntity implements Serializable, Comparable<JobEntity> {
  private static final long serialVersionUID = 7062343992924390450L;
  private Job job;
  private String status;

  private int[] flushPointer;

  public JobEntity() {}

  public JobEntity(Job job) {
    this.job = job;
  }


  public int getJobId() {
    return job.getJobId();
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
      if (this.job.getJobId() == jobEntity.getJobId()) {
        result = true;
      }
    }
    return result;
  }

  @Override
  public int hashCode() {
    return this.job.getJobId();
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this);
  }

  public int[] getFlushPointer() {
    return flushPointer;
  }

  public void setFlushPointer(int[] dimensionsPointer) {
    this.flushPointer = dimensionsPointer;
  }

  @Override
  public int compareTo(JobEntity o) {
    if (this.equals(o)) {
      return 0;
    } else if(this.job.getJobId() < o.getJobId() ){
      return -1;
    }else if(this.job.getJobId() > o.getJobId() ){
      return 1;
    }
    return 0;
  }



}
