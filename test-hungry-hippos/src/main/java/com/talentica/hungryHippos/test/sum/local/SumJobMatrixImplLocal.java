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
