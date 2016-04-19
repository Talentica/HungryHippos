package com.talentica.hungryHippos.tester.web.job.output.service;

import com.talentica.hungryHippos.tester.web.job.data.Job;
import com.talentica.hungryHippos.tester.web.service.ServiceResponse;

public class JobOutputServiceResponse extends ServiceResponse {

	private Job jobDetail;

	public Job getJobDetail() {
		return jobDetail;
	}

	public void setJobDetail(Job jobDetail) {
		this.jobDetail = jobDetail;
	}

}
