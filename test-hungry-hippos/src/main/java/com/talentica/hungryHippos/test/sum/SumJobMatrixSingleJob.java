package com.talentica.hungryHippos.test.sum;

import java.util.ArrayList;
import java.util.List;

import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.client.job.JobMatrix;

public class SumJobMatrixSingleJob implements JobMatrix {

  @Override
  public List<Job> getListOfJobsToExecute() {
    List<Job> jobList = new ArrayList<>();
    jobList.add(new SumJob(new int[] {0}, 3));
    return jobList;
  }

  public static void main(String[] args) {
    System.out.println(new SumJobMatrixSingleJob().getListOfJobsToExecute());
    System.out.println(new SumJobMatrixSingleJob().getListOfJobsToExecute().size());
  }

}
