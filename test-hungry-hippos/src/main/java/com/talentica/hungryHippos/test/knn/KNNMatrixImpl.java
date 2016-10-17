package com.talentica.hungryHippos.test.knn;

import java.util.ArrayList;
import java.util.List;

import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.client.job.JobMatrix;

public class KNNMatrixImpl implements JobMatrix{

  @Override
  public List<Job> getListOfJobsToExecute() {
    List<Job> jobList = new ArrayList<>();
    jobList.add(new KNNJob(new int[]{9},5,6,5,0));
    return jobList;
  }

}
