package com.talentica.hungryHippos.test.utils;

import java.util.List;

import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.client.job.JobMatrix;

public class JobMatrixPrinter {

	public static void main(String[] args)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		if (args.length != 1) {
			System.out.println(
					"Please provide the class name of jobmatrix to print details of e.g. java -cp test-jobs.jar com.talentica.hungryHippos.test.utils.JobMatrixPrinter com.talentica.hungryHippos.test.median.MedianJobMatrixImpl");
		}
		JobMatrix jobMatrix = (JobMatrix) Class.forName(args[0]).newInstance();
		List<Job> jobs = jobMatrix.getListOfJobsToExecute();
		for (Job job : jobs) {
			/*System.out.print("Job with id: " + job.getJobId()
					+ " is available in test job matrix. Job details: Primary dimension-" + job.getPrimaryDimension()
					+ " Dimenstions-");*/
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
