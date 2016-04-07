package com.talentica.hungryHippos.tester.web.job;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.talentica.hungryHippos.tester.web.ServiceError;
import com.talentica.hungryHippos.tester.web.job.data.Job;
import com.talentica.hungryHippos.tester.web.job.data.JobInput;
import com.talentica.hungryHippos.tester.web.job.data.JobInputRepository;
import com.talentica.hungryHippos.tester.web.job.data.JobRepository;

import lombok.Setter;

@Controller
@RequestMapping("/job")
public class JobService {

	@Setter
	@Autowired(required = false)
	private JobRepository jobRepository;

	@Setter
	@Autowired(required = false)
	private JobInputRepository jobInputRepository;

	@RequestMapping(value = "new", method = RequestMethod.POST)
	public @ResponseBody JobServiceResponse create(@RequestBody(required = true) JobServiceRequest request) {
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
	public @ResponseBody JobServiceResponse create(@PathVariable("jobUuid") String jobUuid) {
		JobServiceResponse jobServiceResponse = new JobServiceResponse();
		ServiceError error = validateJobUuid(jobUuid, jobServiceResponse);
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
		jobServiceResponse.setJobDetail(jobDetail);
		return jobServiceResponse;
	}

	private ServiceError getInvalidJobUuidError(String jobUuid) {
		ServiceError error;
		error = new ServiceError();
		error.setMessage("Please provide with valid job UUID. No job with specified id found.");
		error.setDetail("Job UUID:" + jobUuid + " is invalid. Please pass valid job UUID parameter in request.");
		return error;
	}

	private ServiceError validateJobUuid(String jobUuid, JobServiceResponse jobServiceResponse) {
		ServiceError error = null;
		if (StringUtils.isBlank(jobUuid)) {
			error = new ServiceError();
			error.setMessage("Job UUID missing.");
			error.setDetail(
					"Job UUID is blank. Please pass job UUID parameter in request URL e.g. /job/31A74507-4330-4BBD-A419-3FA31B814332");
		}
		return error;
	}

}