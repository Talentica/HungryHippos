package com.talentica.hungryHippos.tester.web.job;

import com.talentica.hungryHippos.tester.web.ServiceError;

import lombok.Getter;
import lombok.Setter;

public class JobServiceResponse {

	@Getter
	@Setter
	private JobDetail jobDetail;

	@Getter
	@Setter
	private ServiceError error;

}
