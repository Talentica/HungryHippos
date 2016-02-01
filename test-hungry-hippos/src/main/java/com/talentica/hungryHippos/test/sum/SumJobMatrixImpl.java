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
		for (int i = 0; i < 1; i++) {
			jobList.add(new SumJob(new int[] { i }, i, 6, jobId++));
			jobList.add(new SumJob(new int[] { i }, i, 7, jobId++));
		}
		return jobList;
	}

}