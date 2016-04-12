package com.talentica.hungryHippos.tester.web.job.service;

import org.joda.time.DateTime;
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
import com.talentica.hungryHippos.tester.web.job.data.STATUS;
import com.talentica.hungryHippos.tester.web.job.output.service.JobOutputService;
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

	@Setter
	@Autowired(required = false)
	private JobOutputService jobOutputService;

	@RequestMapping(value = "new", method = RequestMethod.POST)
	public @ResponseBody JobServiceResponse newJob(@RequestBody(required = true) JobServiceRequest request) {
		JobServiceResponse jobServiceResponse = new JobServiceResponse();
		try {
			ServiceError error = request.validate();

			if (error != null) {
				jobServiceResponse.setError(error);
				return jobServiceResponse;
			}
			Job job = request.getJobDetail();
			JobInput jobInput = job.getJobInput();
			// TODO: Remove hard coding of user id later.
			job.setUserId(1);
			job.setDateTimeSubmitted(DateTime.now().toDate());
			job.setJobOutput(null);
			job.setJobInput(null);
			job.setStatus(STATUS.NOT_STARTED);
			Job savedJob = jobRepository.save(job);
			jobInput.setJob(job);
			jobInputRepository.save(jobInput);
			jobServiceResponse.setJobDetail(savedJob);
		} catch (Exception exception) {
			jobServiceResponse.setError(
					new ServiceError("Error occurred while processing your request. Please try after some time.",
							exception.getMessage()));
		}
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
		jobServiceResponse.setJobDetail(job);
		return jobServiceResponse;
	}

}