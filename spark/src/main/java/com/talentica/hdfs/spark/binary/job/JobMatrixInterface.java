package com.talentica.hdfs.spark.binary.job;

import java.util.List;

import com.talentica.hungryHippos.rdd.job.Job;

public interface JobMatrixInterface {

  public List<Job> getJobs();
  
  public void printMatrix();
}
