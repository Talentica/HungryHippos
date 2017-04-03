/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
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
      System.out.println(
          "Job  :: " + job.toString() + " and dime to flush result :: "
              + Arrays.toString(dimns.stream().mapToInt(i -> i).toArray()));
      dimns.clear();
    }

  }

  public void prepareListOfJobsToExecute() {
    int jobId = 0;
    jobList.add(new SumJob(new int[] {0}, 6,jobId++));
    jobList.add(new SumJob(new int[] {0,1}, 6,jobId++));
    jobList.add(new SumJob(new int[] {0,1}, 7,jobId++));
    jobList.add(new SumJob(new int[] {1}, 6,jobId++));
    jobList.add(new SumJob(new int[] {1,3}, 6,jobId++));
    jobList.add(new SumJob(new int[] {1,3}, 7,jobId++));
  /*  for (int i = 0; i < 3; i++) {
      jobList.add(new SumJob(new int[] {i}, 6, jobId++));
      jobList.add(new SumJob(new int[] {i}, 7, jobId++));
      for (int j = i + 1; j < 4; j++) {
        jobList.add(new SumJob(new int[] {i, j}, 6, jobId++));
        jobList.add(new SumJob(new int[] {i, j}, 7, jobId++));
        for (int k = j + 1; k < 4; k++) {
          jobList.add(new SumJob(new int[] {i, j, k}, 6, jobId++));
          jobList.add(new SumJob(new int[] {i, j, k}, 7, jobId++));
        }
      }
    }*/
  }

}
