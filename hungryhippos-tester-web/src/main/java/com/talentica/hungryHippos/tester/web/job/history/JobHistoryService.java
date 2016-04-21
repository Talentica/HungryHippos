package com.talentica.hungryHippos.tester.web.job.history;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.talentica.hungryHippos.tester.api.Service;
import com.talentica.hungryHippos.tester.web.UserCache;
import com.talentica.hungryHippos.tester.web.job.data.JobRepository;
import com.talentica.hungryHippos.tester.web.user.data.User;

@Controller
@RequestMapping("/job")
public class JobHistoryService extends Service {

	@Autowired(required = false)
	private JobRepository jobRepository;

	@Autowired(required = false)
	private UserCache userCache;

	@RequestMapping(value = "history", method = RequestMethod.GET)
	public @ResponseBody JobHistoryServiceResponse getJobsRecentlyExecutedByUser() {
		User user = userCache.getCurrentLoggedInUser();
		JobHistoryServiceResponse jobHistoryServiceResponse = new JobHistoryServiceResponse();
		jobHistoryServiceResponse.setJobs(jobRepository.findTop5ByUserIdOrderByDateTimeSubmittedDesc(user.getUserId()));
		return jobHistoryServiceResponse;
	}

	public void setJobRepository(JobRepository jobRepository) {
		this.jobRepository = jobRepository;
	}

	public void setUserCache(UserCache userCache) {
		this.userCache = userCache;
	}

}