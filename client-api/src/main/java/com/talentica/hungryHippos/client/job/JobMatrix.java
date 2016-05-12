package com.talentica.hungryHippos.client.job;

import java.util.List;

/**
 * This job matrix interface is a collection of jobs to be executed in one run.
 * 
 * @author nitink
 * @version 0.5.0
 * @since 2016-05-12
 */
public interface JobMatrix {

	/**
	 * List of jobs that are needed while performing the job execution.
	 * 
	 * @return List of the Jobs.
	 */
	public List<Job> getListOfJobsToExecute();

}