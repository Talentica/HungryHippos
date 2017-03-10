/**
 * 
 */
package com.talentica.hungryHippos.rdd.main.job;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author pooshans
 *
 */
public class JobMatrix implements Serializable{
  
  /**
   * 
   */
  private static final long serialVersionUID = -2526185065466488913L;
  private List<Job> jobs;

  public JobMatrix() {
    
  }

  public JobMatrix(List<Job> jobs) {
    this.jobs = jobs;
  }

  public void addJob(Job job) {
    if (jobs == null) {
      jobs = new ArrayList<>();
    }
    jobs.add(job);
  }

  public List<Job> getJobs() {
    return jobs;
  }

  @Override
  public String toString() {
    return "JobConf [jobs=" + jobs + ", getJobs()=" + getJobs() + ", getClass()=" + getClass()
        + ", hashCode()=" + hashCode() + ", toString()=" + super.toString() + "]";
  }

}
