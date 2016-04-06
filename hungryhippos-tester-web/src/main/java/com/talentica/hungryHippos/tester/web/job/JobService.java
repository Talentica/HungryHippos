package com.talentica.hungryHippos.tester.web.job;

import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.talentica.hungryHippos.tester.web.job.data.Job;
import com.talentica.hungryHippos.tester.web.job.data.JobRepository;

import lombok.Setter;

@Controller
@RequestMapping("/job")
public class JobService {

	@Setter
	@Autowired(required = false)
	private JobRepository jobRepository;

	@RequestMapping(method = RequestMethod.POST)
	public @ResponseBody JobServiceResponse create(@RequestBody(required = true) JobServiceRequest request) {
		Job job = request.getJob();
		job.setUuid(UUID.randomUUID().toString().toUpperCase());
		jobRepository.save(job);
		return null;
	}

}