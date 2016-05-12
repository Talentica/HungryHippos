package com.talentica.hungryHippos.client.job;

import java.util.List;

/**
 * Job matrix is a collection of jobs to be executed in one run.
 * 
 * @author nitink
 *
 */
public interface JobMatrix {

	public List<Job> getListOfJobsToExecute();
 
}