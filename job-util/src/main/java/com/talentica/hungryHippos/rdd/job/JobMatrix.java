/**
 * 
 */
package com.talentica.hungryHippos.rdd.job;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.talentica.hdfs.spark.binary.job.JobMatrixInterface;

/**
 * @author pooshans
 *
 */
public class JobMatrix implements JobMatrixInterface,Serializable{
  
  /**
   * 
   */
  private static final long serialVersionUID = -2526185065466488913L;
  private List<Job> jobs;

  public JobMatrix() {
    Job job = new Job(new Integer[] {0,1,2},7,2);
    addJob(job);
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

  @Override
  public void printMatrix() {
    System.out.println(this.toString());
  }

}
