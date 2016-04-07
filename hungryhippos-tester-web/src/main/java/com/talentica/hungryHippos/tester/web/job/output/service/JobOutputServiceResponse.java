package com.talentica.hungryHippos.tester.web.job.output.service;

import com.talentica.hungryHippos.tester.web.job.service.JobDetail;
import com.talentica.hungryHippos.tester.web.service.ServiceResponse;

import lombok.Getter;
import lombok.Setter;

public class JobOutputServiceResponse extends ServiceResponse {

	@Getter
	@Setter
	private JobDetail jobDetail;

}
