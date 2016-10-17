package com.talentica.hungryHippos.test.median;

import java.util.ArrayList;
import java.util.List;

import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.client.job.JobMatrix;

public class MedianJobMatrixImpl implements JobMatrix {

  @Override
  public List<Job> getListOfJobsToExecute() {
    List<Job> jobList = new ArrayList<>();
    int jobId = 0;
    for (int i = 0; i < 3; i++) {
      jobList.add(new MedianJob(new int[] {i}, 6,jobId++));
      jobList.add(new MedianJob(new int[] {i}, 7,jobId++));
      for (int j = i + 1; j < 4; j++) {
        jobList.add(new MedianJob(new int[] {i, j}, 6,jobId++));
        jobList.add(new MedianJob(new int[] {i, j}, 7,jobId++));
        for (int k = j + 1; k < 4; k++) {
          jobList.add(new MedianJob(new int[] {i, j, k}, 6,jobId++));
          jobList.add(new MedianJob(new int[] {i, j, k}, 7,jobId++));
        }
      }
    }
    return jobList;
  }

  public static void main(String[] args) {
    List<Job> listOfJobsToExecute = new MedianJobMatrixImpl().getListOfJobsToExecute();
    System.out.println(listOfJobsToExecute);
    System.out.println(listOfJobsToExecute.size());
  }

}
