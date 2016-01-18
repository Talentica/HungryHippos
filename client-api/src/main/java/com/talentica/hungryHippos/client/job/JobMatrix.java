package com.talentica.hungryHippos.client.job;

import java.util.List;

import com.talentica.hungryHippos.accumulator.Job;

public interface JobMatrix {

	public List<Job> getListOfJobsToExecute();

}