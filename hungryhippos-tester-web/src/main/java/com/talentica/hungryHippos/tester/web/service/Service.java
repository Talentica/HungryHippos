package com.talentica.hungryHippos.tester.web.service;

import org.apache.commons.lang3.StringUtils;

public class Service {

	protected final ServiceError validateUuid(String jobUuid) {
		ServiceError error = null;
		if (StringUtils.isBlank(jobUuid)) {
			error = new ServiceError();
			error.setMessage("Job UUID missing.");
			error.setDetail(
					"Job UUID is blank. Please pass job UUID parameter in request URL e.g. /job/31A74507-4330-4BBD-A419-3FA31B814332");
		}
		return error;
	}

	protected final ServiceError getInvalidJobUuidError(String jobUuid) {
		ServiceError error;
		error = new ServiceError();
		error.setMessage("Please provide with valid job UUID. No job with specified id found.");
		error.setDetail("Job UUID:" + jobUuid + " is invalid. Please pass valid job UUID parameter in request.");
		return error;
	}

}
