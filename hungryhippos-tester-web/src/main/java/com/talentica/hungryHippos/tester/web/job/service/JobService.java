package com.talentica.hungryHippos.tester.web.job.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.talentica.hungryHippos.tester.web.job.data.Job;
import com.talentica.hungryHippos.tester.web.job.data.JobInput;
import com.talentica.hungryHippos.tester.web.job.data.JobInputRepository;
import com.talentica.hungryHippos.tester.web.job.data.JobRepository;
import com.talentica.hungryHippos.tester.web.service.Service;
import com.talentica.hungryHippos.tester.web.service.ServiceError;

import lombok.Setter;

@Controller
@RequestMapping("/job")
public class JobService extends Service {

	@Setter
	@Autowired(required = false)
	private JobRepository jobRepository;

	@Setter
	@Autowired(required = false)
	private JobInputRepository jobInputRepository;

	@RequestMapping(value = "new", method = RequestMethod.POST)
	public @ResponseBody JobServiceResponse newJob(@RequestBody(required = true) JobServiceRequest request) {
		ServiceError error = request.validate();
		JobServiceResponse jobServiceResponse = new JobServiceResponse();
		if (error != null) {
			jobServiceResponse.setError(error);
			return jobServiceResponse;
		}
		Job job = Job.createNewJob();
		Job savedJob = jobRepository.save(job);
		JobInput jobInput = request.getJobDetail().getJobInput();
		jobInput.setJobId(savedJob.getJobId());
		jobInputRepository.save(jobInput);
		jobServiceResponse.setJobDetail(new JobDetail(savedJob, jobInput));
		return jobServiceResponse;
	}

	@RequestMapping(value = "detail/{jobUuid}", method = RequestMethod.GET)
	public @ResponseBody JobServiceResponse getJobDetail(@PathVariable("jobUuid") String jobUuid) {
		JobServiceResponse jobServiceResponse = new JobServiceResponse();
		ServiceError error = validateUuid(jobUuid);
		if (error != null) {
			jobServiceResponse.setError(error);
			return jobServiceResponse;
		}
		Job job = jobRepository.findByUuid(jobUuid);
		if (job == null) {
			error = getInvalidJobUuidError(jobUuid);
			jobServiceResponse.setError(error);
			return jobServiceResponse;
		}
		JobDetail jobDetail = new JobDetail();
		jobDetail.setJob(job);
		jobServiceResponse.setJobDetail(jobDetail);
		return jobServiceResponse;
	}

}