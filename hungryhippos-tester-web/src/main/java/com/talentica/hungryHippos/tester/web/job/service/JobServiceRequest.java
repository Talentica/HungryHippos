package com.talentica.hungryHippos.tester.web.job.service;

import org.apache.commons.lang3.StringUtils;

import com.talentica.hungryHippos.tester.web.job.data.Job;
import com.talentica.hungryHippos.tester.web.service.ServiceError;

import lombok.Getter;
import lombok.Setter;

public class JobServiceRequest {

	@Getter
	@Setter
	private Job jobDetail;

	public ServiceError validate() {
		ServiceError error = null;
		if (jobDetail == null || jobDetail == null || jobDetail.getJobInput() == null) {
			error = new ServiceError("Job information is blank. Please provide with the job information.",
					"Missing jon information in request.");
		}

		if (jobDetail != null && jobDetail.getJobId() != null) {
			error = new ServiceError("Please provide with valid job information.",
					"Job id cannot already set in new job creation request.");
		}

		if (jobDetail != null && StringUtils.isBlank(jobDetail.getUuid())) {
			error = new ServiceError(
					"Please provide with valid job UUID in request. You should get it after successful upload of a valid job jar file.",
					"Job UUID not found.");
		}

		return error;
	}

}
