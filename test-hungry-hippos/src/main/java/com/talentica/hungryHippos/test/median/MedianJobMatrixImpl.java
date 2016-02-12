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
		for (int i = 0; i < 3; i++) {
			jobList.add(new MedianJob(new int[] { i }, i, 6, jobId++));
			jobList.add(new MedianJob(new int[] { i }, i, 7, jobId++));
			for (int j = i + 1; j < 4; j++) {
				jobList.add(new MedianJob(new int[] { i, j }, i, 6, jobId++));
				jobList.add(new MedianJob(new int[] { i, j }, i, 7, jobId++));
				for (int k = j + 1; k < 4; k++) {
					jobList.add(new MedianJob(new int[] { i, j, k }, i, 6, jobId++));
					jobList.add(new MedianJob(new int[] { i, j, k }, i, 7, jobId++));
				}
			}
		}
		return jobList;
	}

	public static void main(String[] args) {
		System.out.println(new MedianJobMatrixImpl().getListOfJobsToExecute().size());
		}

}