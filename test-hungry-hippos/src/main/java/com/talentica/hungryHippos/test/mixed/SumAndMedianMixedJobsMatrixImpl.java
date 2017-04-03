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
package com.talentica.hungryHippos.test.mixed;

import java.util.ArrayList;
import java.util.List;

import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.client.job.JobMatrix;
import com.talentica.hungryHippos.test.median.MedianJob;
import com.talentica.hungryHippos.test.sum.SumJob;

public class SumAndMedianMixedJobsMatrixImpl implements JobMatrix {

  @Override
  public List<Job> getListOfJobsToExecute() {
    List<Job> jobList = new ArrayList<>();
    int jobId = 0;
    for (int i = 0; i < 3; i++) {
      jobList.add(new SumJob(new int[] {i}, 6,jobId++));
      jobList.add(new MedianJob(new int[] {i}, 7,jobId++));
      for (int j = i + 1; j < 4; j++) {
        jobList.add(new SumJob(new int[] {i, j}, 6,jobId++));
        jobList.add(new MedianJob(new int[] {i, j}, 7,jobId++));
        for (int k = j + 1; k < 4; k++) {
          jobList.add(new SumJob(new int[] {i, j, k}, 6,jobId++));
          jobList.add(new MedianJob(new int[] {i, j, k}, 7,jobId++));
        }
      }
    }
    return jobList;
  }

  public static void main(String[] args) {
    System.out.println(new SumAndMedianMixedJobsMatrixImpl().getListOfJobsToExecute().size());
  }

}
