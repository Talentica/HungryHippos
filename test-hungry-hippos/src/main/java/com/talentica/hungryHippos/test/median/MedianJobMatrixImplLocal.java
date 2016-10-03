package com.talentica.hungryHippos.test.median;

import java.util.ArrayList;
import java.util.List;

import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.client.job.JobMatrix;

public class MedianJobMatrixImplLocal implements JobMatrix {

  @Override
  public List<Job> getListOfJobsToExecute() {
    List<Job> jobList = new ArrayList<>();
    for (int i = 0; i < 1; i++) {
      jobList.add(new MedianJob(new int[] {i}, 6));
      jobList.add(new MedianJob(new int[] {i}, 7));
      for (int j = i + 1; j < 4; j++) {
        jobList.add(new MedianJob(new int[] {i, j}, 6));
        jobList.add(new MedianJob(new int[] {i, j}, 7));
        for (int k = j + 1; k < 4; k++) {
          jobList.add(new MedianJob(new int[] {i, j, k}, 6));
          jobList.add(new MedianJob(new int[] {i, j, k}, 7));
        }
      }
    }
    return jobList;
  }

  public static void main(String[] args) {
    System.out.println(new MedianJobMatrixImplLocal().getListOfJobsToExecute());
    System.out.println(new MedianJobMatrixImplLocal().getListOfJobsToExecute().size());
  }

}
