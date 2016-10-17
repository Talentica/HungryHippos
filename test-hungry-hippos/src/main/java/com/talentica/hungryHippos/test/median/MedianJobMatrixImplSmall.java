package com.talentica.hungryHippos.test.median;

import java.util.ArrayList;
import java.util.List;

import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.client.job.JobMatrix;

public class MedianJobMatrixImplSmall implements JobMatrix {

  @Override
  public List<Job> getListOfJobsToExecute() {
    List<Job> jobList = new ArrayList<>();
    int jobId = 0;
    jobList.add(new MedianJob(new int[] {0, 1, 3}, 6,jobId++));
    jobList.add(new MedianJob(new int[] {0, 1, 3}, 7,jobId++));
    return jobList;
  }

  public static void main(String[] args) {
    System.out.println(new MedianJobMatrixImplSmall().getListOfJobsToExecute());
    System.out.println(new MedianJobMatrixImplSmall().getListOfJobsToExecute().size());
  }

}
