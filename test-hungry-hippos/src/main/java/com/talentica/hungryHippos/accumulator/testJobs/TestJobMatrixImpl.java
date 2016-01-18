package com.talentica.hungryHippos.accumulator.testJobs;

import java.util.ArrayList;
import java.util.List;

import com.talentica.hungryHippos.accumulator.Job;
import com.talentica.hungryHippos.client.job.JobMatrix;

public class TestJobMatrixImpl implements JobMatrix {

	private List<Job> jobList = new ArrayList<>();

	@Override
	public List<Job> getListOfJobsToExecute() {
		int jobId = 0;
		for (int i = 0; i < 3; i++) {
			jobList.add(new TestJob(new int[] { i }, i, 6, jobId++));
			jobList.add(new TestJob(new int[] { i }, i, 7, jobId++));
			for (int j = i + 1; j < 5; j++) {
				jobList.add(new TestJob(new int[] { i, j }, i, 6, jobId++));
				jobList.add(new TestJob(new int[] { i, j }, j, 7, jobId++));
				for (int k = j + 1; k < 5; k++) {
					jobList.add(new TestJob(new int[] { i, j, k }, i, 6, jobId++));
					jobList.add(new TestJob(new int[] { i, j, k }, j, 7, jobId++));
				}
			}
		}
		return jobList;
	}

}