package com.talentica.hungryHippos.tester.web.job.history;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.talentica.hungryHippos.tester.web.job.data.JobRepository;
import com.talentica.hungryHippos.tester.web.service.Service;

import lombok.Setter;

@Controller
@RequestMapping("/job")
public class JobHistoryService extends Service {

	@Setter
	@Autowired(required = false)
	private JobRepository jobRepository;

	@RequestMapping(value = "history/{userId}", method = RequestMethod.GET)
	public @ResponseBody JobHistoryServiceResponse getJobsRecentlyExecutedByUser(
			@PathVariable("userId") Integer userId) {
		JobHistoryServiceResponse jobHistoryServiceResponse = new JobHistoryServiceResponse();
		jobHistoryServiceResponse.setJobs(jobRepository.findTop5ByUserIdOrderByDateTimeSubmittedDesc(userId));
		return jobHistoryServiceResponse;
	}

}