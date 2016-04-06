package com.talentica.hungryHippos.tester.web.job;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import com.talentica.hungryHippos.tester.web.job.data.JobRepository;

@Controller
@RequestMapping("/job")
public class JobService {

	@Autowired(required = false)
	private JobRepository jobRepository;

}