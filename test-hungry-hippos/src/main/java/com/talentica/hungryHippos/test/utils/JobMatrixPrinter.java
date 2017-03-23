/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
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
package com.talentica.hungryHippos.test.utils;

import java.util.List;

import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.client.job.JobMatrix;

public class JobMatrixPrinter {

	public static void main(String[] args)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		if (args.length != 1) {
			System.out.println(
					"Please provide the class name of jobmatrix to print details of e.g. java -cp test-jobs.jar com.talentica.hungryHippos.test.utils.JobMatrixPrinter com.talentica.hungryHippos.test.median.MedianJobMatrixImpl");
		}
		JobMatrix jobMatrix = (JobMatrix) Class.forName(args[0]).newInstance();
		List<Job> jobs = jobMatrix.getListOfJobsToExecute();
		for (Job job : jobs) {
			/*System.out.print("Job with id: " + job.getJobId()
					+ " is available in test job matrix. Job details: Primary dimension-" + job.getPrimaryDimension()
					+ " Dimenstions-");*/
			boolean putComma = false;
			for (int dimensionIndex : job.getDimensions()) {
				if (putComma) {
					System.out.print(",");
				}
				System.out.print(dimensionIndex);
				putComma = true;
			}
			System.out.println();
		}
	}

}
