/**
 * 
 */
package com.talentica.hungryHippos.test.median;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.test.sum.SumJob;

/**
 * @author pooshans
 *
 */
public class SortedDataJobTest {


  private int[] shardingDime;
  private List<Job> jobList;

  @Before
  public void setUp() {
    shardingDime = new int[] {0, 1, 2};
    jobList = new ArrayList<>();
    prepareListOfJobsToExecute();
  }

  @Test
  public void testJobsDimensionsFlush() {
    List<Integer> dimns = new ArrayList<>();
    for (Job job : jobList) {
      for (int jobDim = 0; jobDim < job.getDimensions().length; jobDim++) {
        for (int index = 0; index < shardingDime.length; index++) {
          if (job.getDimensions()[jobDim] == shardingDime[index]) {
            dimns.add(job.getDimensions()[jobDim]);
          }
        }
      }
      System.out.println("Job dimes :: " + Arrays.toString(job.getDimensions()) + " and dime to flush result :: " + Arrays.toString(dimns.stream().mapToInt(i -> i).toArray()));
      dimns.clear();
    }

  }

  public void prepareListOfJobsToExecute() {
    for (int i = 0; i < 3; i++) {
      jobList.add(new SumJob(new int[] {i}, 6));
      jobList.add(new SumJob(new int[] {i}, 7));
      for (int j = i + 1; j < 4; j++) {
        jobList.add(new SumJob(new int[] {i, j}, 6));
        jobList.add(new SumJob(new int[] {i, j}, 7));
        for (int k = j + 1; k < 4; k++) {
          jobList.add(new SumJob(new int[] {i, j, k}, 6));
          jobList.add(new SumJob(new int[] {i, j, k}, 7));
        }
      }
    }
  }

}
