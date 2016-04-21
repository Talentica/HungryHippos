package com.talentica.hungryHippos.tester.api.job;

import com.talentica.hungryHippos.tester.api.ServiceResponse;

public class JobServiceResponse extends ServiceResponse {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Job jobDetail;

	public Job getJobDetail() {
		return jobDetail;
	}

	public void setJobDetail(Job jobDetail) {
		this.jobDetail = jobDetail;
	}

}
