package com.talentica.hungryHippos.test.sum;

import java.util.ArrayList;
import java.util.List;

import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.client.job.JobMatrix;

public class SumJobMatrixImplSmall implements JobMatrix {

  @Override
  public List<Job> getListOfJobsToExecute() {
    List<Job> jobList = new ArrayList<>();
    jobList.add(new SumJob(new int[] {0, 1, 3}, 6));
    jobList.add(new SumJob(new int[] {0, 1, 3}, 7));
    return jobList;
  }

  public static void main(String[] args) {
    System.out.println(new SumJobMatrixImplSmall().getListOfJobsToExecute());
    System.out.println(new SumJobMatrixImplSmall().getListOfJobsToExecute().size());
  }

}
