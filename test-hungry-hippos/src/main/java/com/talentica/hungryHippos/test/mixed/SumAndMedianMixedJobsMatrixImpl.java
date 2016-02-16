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
		for (int i = 0; i < 3; i++) {
			jobList.add(new SumJob(new int[] { i }, i, 6));
			jobList.add(new MedianJob(new int[] { i }, i, 7));
			for (int j = i + 1; j < 4; j++) {
				jobList.add(new SumJob(new int[] { i, j }, i, 6));
				jobList.add(new MedianJob(new int[] { i, j }, i, 7));
				for (int k = j + 1; k < 4; k++) {
					jobList.add(new SumJob(new int[] { i, j, k }, i, 6));
					jobList.add(new MedianJob(new int[] { i, j, k }, i, 7));
				}
			}
		}
		return jobList;
	}

	public static void main(String[] args) {
		System.out.println(new SumAndMedianMixedJobsMatrixImpl().getListOfJobsToExecute().size());
	}

}