/**
 * 
 */
package com.talentica.hungryHippos.rdd.job;

import java.util.ArrayList;
import java.util.List;

import com.talentica.hdfs.spark.binary.job.JobMatrixInterface;

/**
 * @author pooshans
 *
 */
public class JobMatrix implements JobMatrixInterface{

  private List<Job> jobs = new ArrayList<Job>();
  
  @Override
  public List<Job> getJobs() {
     jobs.add(new Job(new Integer[] {0}, 6, 0));
     return jobs;
  }

  @Override
  public void printMatrix() {
    jobs.toString();
  }

}
