/**
 * 
 */
package com.talentica.hungryHippos.rdd.utility;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import com.talentica.hungryHippos.rdd.job.Job;

/**
 * @author pooshans
 *
 */
public class BroadcastVariable {

  private JavaSparkContext context;


  private Broadcast<Job> job;
  private Broadcast<Integer> jobPrimaryDimension;

  public BroadcastVariable(JavaSparkContext context) {
    this.context = context;
  }

  public Broadcast<Job> getJob() {
    return job;
  }

  public void setJob(Job job) {
    this.job = context.broadcast(job);
  }

  public Broadcast<Integer> getJobPrimaryDimension() {
    return jobPrimaryDimension;
  }

  public void setJobPrimaryDimension(int jobPrimaryDimension) {
    this.jobPrimaryDimension = context.broadcast(jobPrimaryDimension);
  }

}
