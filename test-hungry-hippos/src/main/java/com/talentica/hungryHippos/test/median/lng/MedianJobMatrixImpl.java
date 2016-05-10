package com.talentica.hungryHippos.test.median.lng;

import java.util.ArrayList;
import java.util.List;

import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.client.job.JobMatrix;

public class MedianJobMatrixImpl implements JobMatrix {

	@Override
	public List<Job> getListOfJobsToExecute() {
		List<Job> jobList = new ArrayList<>();
		for (int i = 0; i < 1; i++) {
			jobList.add(new MedianJob(new int[] { i }, i, 1));
		}
		return jobList;
	}

	public static void main(String[] args) {
		List<Job> listOfJobsToExecute = new MedianJobMatrixImpl().getListOfJobsToExecute();
		System.out.println(listOfJobsToExecute);
		System.out.println(listOfJobsToExecute.size());
	}

}