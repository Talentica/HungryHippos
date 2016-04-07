package com.talentica.hungryHippos.tester.web.job.service;

import com.talentica.hungryHippos.tester.web.service.ServiceError;

import lombok.Getter;
import lombok.Setter;

public class JobServiceRequest {

	@Getter
	@Setter
	private JobDetail jobDetail;

	public ServiceError validate() {
		ServiceError error = null;
		if (jobDetail == null || jobDetail.getJobInput() == null) {
			error = new ServiceError("Job information is blank. Please provide with the job information.",
					"Missing jon information in request.");
		}

		if (jobDetail.getJob() != null && jobDetail.getJob().getJobId() != null) {
			error = new ServiceError("Please provide with valid job information.",
					"Job id cannot already set in new job creation request.");
		}
		return error;
	}

}
