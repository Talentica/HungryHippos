package com.talentica.hungryHippos.tester.web.job;

import com.talentica.hungryHippos.tester.web.ServiceError;
import com.talentica.hungryHippos.tester.web.job.data.Job;

import lombok.Getter;
import lombok.Setter;

public class JobServiceResponse {

	@Getter
	@Setter
	private Job job;

	@Getter
	@Setter
	private ServiceError error;

}
