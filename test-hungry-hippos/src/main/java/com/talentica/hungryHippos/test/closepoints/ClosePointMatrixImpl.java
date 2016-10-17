package com.talentica.hungryHippos.test.closepoints;

import java.util.ArrayList;
import java.util.List;

import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.client.job.JobMatrix;

public class ClosePointMatrixImpl implements JobMatrix{

  @Override
  public List<Job> getListOfJobsToExecute() {
    List<Job> jobList = new ArrayList<>();
    jobList.add(new ClosePointsJob(new int[]{9},5,6,41.499914,-81.559426,0));
    return jobList;
  }

  public static void main(String[] args){
    System.out.println(new ClosePointMatrixImpl().getListOfJobsToExecute());
  }
}
