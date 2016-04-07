package com.talentica.hungryHippos.tester.web.job.status;

import java.util.List;

import com.talentica.hungryHippos.tester.web.job.JobDetail;
import com.talentica.hungryHippos.tester.web.service.ServiceResponse;

import lombok.Getter;
import lombok.Setter;

public class JobStatusServiceResponse extends ServiceResponse {

	@Getter
	@Setter
	private JobDetail jobDetail;

	@Getter
	@Setter
	private List<ProcessInstance> processInstances;

}
