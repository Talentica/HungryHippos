package com.talentica.hungryHippos.tester.web.job.output;

import com.talentica.hungryHippos.tester.web.job.JobDetail;
import com.talentica.hungryHippos.tester.web.service.ServiceResponse;

import lombok.Getter;
import lombok.Setter;

public class JobOutputServiceResponse extends ServiceResponse {

	@Getter
	@Setter
	private JobDetail jobDetail;

}
