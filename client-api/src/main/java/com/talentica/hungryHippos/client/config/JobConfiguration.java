/**
 * 
 */
package com.talentica.hungryHippos.client.config;

import java.util.ArrayList;
import java.util.List;

public class JobConfiguration {

  private List<Job> jobs;

  public JobConfiguration() {}

  public JobConfiguration(List<Job> jobs) {
    this.jobs = jobs;
  }

  public void addJob(Job job) {
    if (jobs == null) {
      jobs = new ArrayList<>();
    }
    jobs.add(job);
  }

  public List<PrimaryDimensionwiseJobsCollection> getJobsByPrimaryDimension() {
    return PrimaryDimensionwiseJobsCollection.from(jobs, new int[] {0, 1, 2});
  }
  

}
