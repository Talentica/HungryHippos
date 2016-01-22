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
		for (int i = 0; i < 1; i++) {
			jobList.add(new MedianJob(new int[] { i }, i, 6, jobId++));
			jobList.add(new MedianJob(new int[] { i }, i, 7, jobId++));
		}
		return jobList;
	}

}