package com.talentica.hungryHippos.tester.web.job.history;

import java.util.List;

import com.talentica.hungryHippos.tester.api.ServiceResponse;
import com.talentica.hungryHippos.tester.api.job.Job;

public class JobHistoryServiceResponse extends ServiceResponse {

	private static final long serialVersionUID = 1L;

	private List<Job> jobs;

	public List<Job> getJobs() {
		return jobs;
	}

	public void setJobs(List<Job> jobs) {
		this.jobs = jobs;
		if (jobs != null) {
			jobs.forEach(job -> job.setExecutionTimeInSeconds());
		}
	}

}
