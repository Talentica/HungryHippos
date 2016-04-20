package com.talentica.hungryHippos.tester.web.job.output.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.talentica.hungryHippos.tester.web.UserCache;
import com.talentica.hungryHippos.tester.web.job.data.Job;
import com.talentica.hungryHippos.tester.web.job.data.JobRepository;
import com.talentica.hungryHippos.tester.web.service.Service;
import com.talentica.hungryHippos.tester.web.service.ServiceError;

@Controller
@RequestMapping("/job")
public class JobOutputService extends Service {

	@Autowired(required = false)
	private JobRepository jobRepository;

	@Autowired(required = false)
	private UserCache userCache;

	@RequestMapping(value = "output/detail/{jobUuid}", method = RequestMethod.GET)
	public @ResponseBody JobOutputServiceResponse getJobOutputDetail(@PathVariable("jobUuid") String jobUuid) {
		JobOutputServiceResponse jobStatusServiceResponse = new JobOutputServiceResponse();
		ServiceError error = validateUuid(jobUuid);
		if (error != null) {
			jobStatusServiceResponse.setError(error);
			return jobStatusServiceResponse;
		}
		Job job = jobRepository.findByUuidAndUserId(jobUuid, userCache.getCurrentLoggedInUser().getUserId());
		if (job == null) {
			error = getInvalidJobUuidError(jobUuid);
			jobStatusServiceResponse.setError(error);
			return jobStatusServiceResponse;
		}
		jobStatusServiceResponse.setJobDetail(job);
		return jobStatusServiceResponse;
	}

	public void setUserCache(UserCache userCache) {
		this.userCache = userCache;
	}

	public void setJobRepository(JobRepository jobRepository) {
		this.jobRepository = jobRepository;
	}
}