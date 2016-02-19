package com.talentica.hungryHippos.test.sum;

import java.util.ArrayList;
import java.util.List;

import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.client.job.JobMatrix;

public class SumJobMatrixImpl implements JobMatrix {

	@Override
	public List<Job> getListOfJobsToExecute() {
		List<Job> jobList = new ArrayList<>();
		for (int i = 0; i < 1; i++) {
			jobList.add(new SumJob(new int[] { i }, i, 6));
			jobList.add(new SumJob(new int[] { i }, i, 7));
		}
		return jobList;
	}

	public static void main(String[] args) {
		System.out.println(new SumJobMatrixImpl().getListOfJobsToExecute());
		System.out.println(new SumJobMatrixImpl().getListOfJobsToExecute().size());
	}

}