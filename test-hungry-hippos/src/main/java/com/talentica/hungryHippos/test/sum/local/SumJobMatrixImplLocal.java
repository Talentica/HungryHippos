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
    for (int i = 0; i < 1; i++) {
      for (int j = i + 1; j < 4; j++) {
        for (int k = j + 2; k < 4; k++) {
          jobList.add(new SumJobLocal(new int[] {i, j, k}, 6,jobId++));
          jobList.add(new SumJobLocal(new int[] {i, j, k}, 7,jobId++));
          jobList.add(new SumJobLocal(new int[] {i, k + 1}, 6,jobId++));
          jobList.add(new SumJobLocal(new int[] {i, k + 1}, 7,jobId++));
        }
      }
    }
    return jobList;
  }

  public static void main(String[] args) {
    System.out.println(new SumJobMatrixImplLocal().getListOfJobsToExecute());
    System.out.println(new SumJobMatrixImplLocal().getListOfJobsToExecute().size());
  }

}
