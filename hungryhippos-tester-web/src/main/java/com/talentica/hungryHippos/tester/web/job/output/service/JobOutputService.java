package com.talentica.hungryHippos.tester.web.job.output.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.talentica.hungryHippos.tester.web.job.data.Job;
import com.talentica.hungryHippos.tester.web.job.data.JobRepository;
import com.talentica.hungryHippos.tester.web.job.output.data.JobOutputRepository;
import com.talentica.hungryHippos.tester.web.service.Service;
import com.talentica.hungryHippos.tester.web.service.ServiceError;

import lombok.Setter;

@Controller
@RequestMapping("/job")
public class JobOutputService extends Service {

	@Setter
	@Autowired(required = false)
	private JobRepository jobRepository;

	@Setter
	@Autowired(required = false)
	private JobOutputRepository jobOutputRepository;

	@RequestMapping(value = "output/detail/{jobUuid}", method = RequestMethod.GET)
	public @ResponseBody JobOutputServiceResponse getJobOutputDetail(@PathVariable("jobUuid") String jobUuid) {
		JobOutputServiceResponse jobStatusServiceResponse = new JobOutputServiceResponse();
		ServiceError error = validateUuid(jobUuid);
		if (error != null) {
			jobStatusServiceResponse.setError(error);
			return jobStatusServiceResponse;
		}
		Job job = jobRepository.findByUuid(jobUuid);
		if (job == null) {
			error = getInvalidJobUuidError(jobUuid);
			jobStatusServiceResponse.setError(error);
			return jobStatusServiceResponse;
		}
		jobStatusServiceResponse.setJobDetail(job);
		return jobStatusServiceResponse;
	}

}