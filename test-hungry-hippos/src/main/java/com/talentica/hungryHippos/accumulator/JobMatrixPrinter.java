package com.talentica.hungryHippos.accumulator;

import java.util.List;

import com.talentica.hungryHippos.accumulator.testJobs.TestJobMatrixImpl;

public class JobMatrixPrinter {

	public static void main(String[] args) {
		TestJobMatrixImpl jobMatrixImpl = new TestJobMatrixImpl();
		List<Job> jobs = jobMatrixImpl.getListOfJobsToExecute();
		for (Job job : jobs) {
			System.out.print("Job with id: " + job.getJobId()
					+ " is available in test job matrix. Job details: Primary dimension-" + job.getPrimaryDimension()
					+ " Dimenstions-");
			boolean putComma = false;
			for (int dimensionIndex : job.getDimensions()) {
				if (putComma) {
					System.out.print(",");
				}
				System.out.print(dimensionIndex);
				putComma = true;
			}
			System.out.println();
		}
	}

}
