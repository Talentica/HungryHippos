package com.talentica.hungryHippos.test.sum.local;

import java.util.ArrayList;
import java.util.List;

import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.client.job.JobMatrix;

public class SumJobMatrixImplLocal implements JobMatrix {

  @Override
  public List<Job> getListOfJobsToExecute() {
    List<Job> jobList = new ArrayList<>();
    int jobId = 0;
    jobList.add(new SumJobLocal(new int[] {0, 5}, 3, jobId++));
    jobList.add(new SumJobLocal(new int[] {0, 5}, 4, jobId++));

    jobList.add(new SumJobLocal(new int[] {0, 6}, 3, jobId++));
    jobList.add(new SumJobLocal(new int[] {0, 6}, 4, jobId++));

    return jobList;
  }

  public static void main(String[] args) {
    System.out.println(new SumJobMatrixImplLocal().getListOfJobsToExecute());
    System.out.println(new SumJobMatrixImplLocal().getListOfJobsToExecute().size());
  }

}
