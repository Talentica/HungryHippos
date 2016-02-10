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
		int numberOfKeys = 3;
		for (int i = 0; i < numberOfKeys; i++) {
			jobList.add(new SumJob(new int[] { i }, i, 6, jobId++));
			jobList.add(new SumJob(new int[] { i }, i, 7, jobId++));
			for (int j = i + 1; j < numberOfKeys - 1; j++) {

				jobList.add(new SumJob(new int[] { i, j }, j, 6, jobId++));
				jobList.add(new SumJob(new int[] { i, j }, j, 7, jobId++));

				jobList.add(new SumJob(new int[] { i, j }, i, 6, jobId++));
				jobList.add(new SumJob(new int[] { i, j }, i, 7, jobId++));
				for (int k = j + 1; k < numberOfKeys; k++) {

					jobList.add(new SumJob(new int[] { k }, k, 6, jobId++));
					jobList.add(new SumJob(new int[] { k }, k, 7, jobId++));

					jobList.add(new SumJob(new int[] { j, k }, j, 6, jobId++));
					jobList.add(new SumJob(new int[] { j, k }, j, 7, jobId++));

					jobList.add(new SumJob(new int[] { j, k }, k, 6, jobId++));
					jobList.add(new SumJob(new int[] { j, k }, k, 7, jobId++));

					jobList.add(new SumJob(new int[] { i, k }, i, 6, jobId++));
					jobList.add(new SumJob(new int[] { i, k }, i, 7, jobId++));

					jobList.add(new SumJob(new int[] { i, k }, k, 6, jobId++));
					jobList.add(new SumJob(new int[] { i, k }, k, 7, jobId++));

					jobList.add(new SumJob(new int[] { i, j, k }, i, 6, jobId++));
					jobList.add(new SumJob(new int[] { i, j, k }, i, 7, jobId++));

					jobList.add(new SumJob(new int[] { i, j, k }, j, 6, jobId++));
					jobList.add(new SumJob(new int[] { i, j, k }, j, 7, jobId++));

					jobList.add(new SumJob(new int[] { i, j, k }, k, 6, jobId++));
					jobList.add(new SumJob(new int[] { i, j, k }, k, 7, jobId++));
				}
			}
		}
		return jobList;
	}

	public static void main(String[] args) {
		System.out.println(new SumJobMatrixImpl().getListOfJobsToExecute());
	}

}