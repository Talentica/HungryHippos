package com.talentica.hungryHippos.tester.web.job.history;

import java.util.List;

import com.talentica.hungryHippos.tester.web.job.data.Job;
import com.talentica.hungryHippos.tester.web.service.ServiceResponse;

public class JobHistoryServiceResponse extends ServiceResponse {

	private List<Job> jobs;

	public List<Job> getJobs() {
		return jobs;
	}

	public void setJobs(List<Job> jobs) {
		this.jobs = jobs;
	}

}
