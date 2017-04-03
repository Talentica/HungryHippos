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
package com.talentica.hungryHippos.test.median.lng;

import java.util.ArrayList;
import java.util.List;

import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.client.job.JobMatrix;

public class MedianJobMatrixImpl implements JobMatrix {

	@Override
	public List<Job> getListOfJobsToExecute() {
		List<Job> jobList = new ArrayList<>();
		int jobId = 0;
		for (int i = 0; i < 1; i++) {
			jobList.add(new MedianJob(new int[] { i }, i, 1,jobId++));
		}
		return jobList;
	}

	public static void main(String[] args) {
		List<Job> listOfJobsToExecute = new MedianJobMatrixImpl().getListOfJobsToExecute();
		System.out.println(listOfJobsToExecute);
		System.out.println(listOfJobsToExecute.size());
	}

}