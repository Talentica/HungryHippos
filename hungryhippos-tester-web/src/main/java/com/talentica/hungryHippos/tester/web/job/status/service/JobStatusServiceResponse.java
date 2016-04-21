package com.talentica.hungryHippos.tester.web.job.status.service;

import java.util.List;

import com.talentica.hungryHippos.tester.api.ServiceResponse;
import com.talentica.hungryHippos.tester.web.job.data.Job;
import com.talentica.hungryHippos.tester.web.job.status.data.ProcessInstance;

public class JobStatusServiceResponse extends ServiceResponse {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Job jobDetail;

	private List<ProcessInstance> processInstances;

	public Job getJobDetail() {
		return jobDetail;
	}

	public void setJobDetail(Job jobDetail) {
		this.jobDetail = jobDetail;
	}

	public List<ProcessInstance> getProcessInstances() {
		return processInstances;
	}

	public void setProcessInstances(List<ProcessInstance> processInstances) {
		this.processInstances = processInstances;
	}

}
