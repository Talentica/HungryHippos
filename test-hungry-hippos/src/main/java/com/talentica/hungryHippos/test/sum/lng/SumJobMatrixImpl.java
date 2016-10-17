package com.talentica.hungryHippos.test.sum.lng;

import java.util.ArrayList;
import java.util.List;

import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.client.job.JobMatrix;

public class SumJobMatrixImpl implements JobMatrix {

	@Override
	public List<Job> getListOfJobsToExecute() {
		List<Job> jobList = new ArrayList<>();
		int jobId = 0;
		jobList.add(new SumJob(new int[] { 0 }, 0, 1,jobId++));
		return jobList;
	}

	public static void main(String[] args) {
		System.out.println(new SumJobMatrixImpl().getListOfJobsToExecute());
		System.out.println(new SumJobMatrixImpl().getListOfJobsToExecute().size());
	}

}