package com.talentica.hungryHippos.test.sum;

import java.util.ArrayList;
import java.util.List;

import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.client.job.JobMatrix;

public class SumJobMatrixImpl implements JobMatrix {

	@Override
	public List<Job> getListOfJobsToExecute() {
		List<Job> jobList = new ArrayList<>();
		int jobId = 0;
		int numberOfKeys = 2;
		for (int i = 0; i < numberOfKeys - 1; i++) {
			jobList.add(new SumJob(new int[] { i }, i, 6, jobId++));
			jobList.add(new SumJob(new int[] { i }, i, 7, jobId++));
			for (int j = i + 1; j < numberOfKeys; j++) {
				jobList.add(new SumJob(new int[] { i, j }, i, 6, jobId++));
				jobList.add(new SumJob(new int[] { i, j }, i, 7, jobId++));
				for (int k = j + 1; k < numberOfKeys; k++) {
					jobList.add(new SumJob(new int[] { i, j, k }, i, 6, jobId++));
					jobList.add(new SumJob(new int[] { i, j, k }, i, 7, jobId++));
				}
			}
		}
		return jobList;
	}

}