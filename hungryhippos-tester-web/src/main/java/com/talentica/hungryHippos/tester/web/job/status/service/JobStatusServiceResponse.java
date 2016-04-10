package com.talentica.hungryHippos.tester.web.job.status.service;

import java.util.List;

import com.talentica.hungryHippos.tester.web.job.data.Job;
import com.talentica.hungryHippos.tester.web.job.status.data.ProcessInstance;
import com.talentica.hungryHippos.tester.web.service.ServiceResponse;

import lombok.Getter;
import lombok.Setter;

public class JobStatusServiceResponse extends ServiceResponse {

	@Getter
	@Setter
	private Job jobDetail;

	@Getter
	@Setter
	private List<ProcessInstance> processInstances;

}
