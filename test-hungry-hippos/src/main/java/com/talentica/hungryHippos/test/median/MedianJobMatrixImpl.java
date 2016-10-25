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

    jobList.add(new MedianJob(new int[] {0}, 3, jobId++));
    jobList.add(new MedianJob(new int[] {0}, 4, jobId++));

    jobList.add(new MedianJob(new int[] {1}, 3, jobId++));
    jobList.add(new MedianJob(new int[] {1}, 4, jobId++));

    jobList.add(new MedianJob(new int[] {2}, 3, jobId++));
    jobList.add(new MedianJob(new int[] {2}, 4, jobId++));


    jobList.add(new MedianJob(new int[] {0, 1}, 3, jobId++));
    jobList.add(new MedianJob(new int[] {0, 1}, 4, jobId++));

    jobList.add(new MedianJob(new int[] {0, 2}, 3, jobId++));
    jobList.add(new MedianJob(new int[] {0, 2}, 4, jobId++));

    jobList.add(new MedianJob(new int[] {1, 2}, 3, jobId++));
    jobList.add(new MedianJob(new int[] {1, 2}, 4, jobId++));

    jobList.add(new MedianJob(new int[] {0, 1, 2}, 3, jobId++));
    jobList.add(new MedianJob(new int[] {0, 1, 2}, 4, jobId++));

    jobList.add(new MedianJob(new int[] {0, 5}, 3, jobId++));
    jobList.add(new MedianJob(new int[] {0, 5}, 4, jobId++));

    jobList.add(new MedianJob(new int[] {0, 1, 5}, 3, jobId++));
    jobList.add(new MedianJob(new int[] {0, 1, 5}, 4, jobId++));

    jobList.add(new MedianJob(new int[] {0, 1, 2, 5}, 3, jobId++));
    jobList.add(new MedianJob(new int[] {0, 1, 2, 5}, 4, jobId++));

    jobList.add(new MedianJob(new int[] {1, 2, 6}, 3, jobId++));
    jobList.add(new MedianJob(new int[] {1, 2, 6}, 4, jobId++));

    jobList.add(new MedianJob(new int[] {0, 6}, 3, jobId++));
    jobList.add(new MedianJob(new int[] {0, 6}, 4, jobId++));

    jobList.add(new MedianJob(new int[] {2, 6}, 3, jobId++));
    jobList.add(new MedianJob(new int[] {2, 6}, 4, jobId++));

    jobList.add(new MedianJob(new int[] {1, 6}, 3, jobId++));
    jobList.add(new MedianJob(new int[] {1, 6}, 4, jobId++));

    return jobList;
    }

  public static void main(String[] args) {
    System.out.println(new MedianJobMatrixImpl().getListOfJobsToExecute());
    System.out.println(new MedianJobMatrixImpl().getListOfJobsToExecute().size());
  }
}
