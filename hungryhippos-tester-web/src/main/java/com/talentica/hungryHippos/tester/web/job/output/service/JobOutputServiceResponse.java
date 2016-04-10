package com.talentica.hungryHippos.tester.web.job.output.service;

import com.talentica.hungryHippos.tester.web.job.data.Job;
import com.talentica.hungryHippos.tester.web.service.ServiceResponse;

import lombok.Getter;
import lombok.Setter;

public class JobOutputServiceResponse extends ServiceResponse {

	@Getter
	@Setter
	private Job jobDetail;

}
