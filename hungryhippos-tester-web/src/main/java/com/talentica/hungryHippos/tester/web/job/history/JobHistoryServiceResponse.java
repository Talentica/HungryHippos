package com.talentica.hungryHippos.tester.web.job.history;

import java.util.List;

import com.talentica.hungryHippos.tester.web.job.data.Job;
import com.talentica.hungryHippos.tester.web.service.ServiceResponse;

import lombok.Getter;
import lombok.Setter;

public class JobHistoryServiceResponse extends ServiceResponse {

	@Getter
	@Setter
	private List<Job> jobs;

}
