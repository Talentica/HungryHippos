package com.talentica.hungryHippos.tester.web.job.service;

import com.talentica.hungryHippos.tester.web.service.ServiceResponse;

import lombok.Getter;
import lombok.Setter;

public class JobServiceResponse extends ServiceResponse {

	@Getter
	@Setter
	private JobDetail jobDetail;

}
