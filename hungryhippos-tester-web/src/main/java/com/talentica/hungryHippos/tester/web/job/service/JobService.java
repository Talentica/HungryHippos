package com.talentica.hungryHippos.tester.web.job.service;

import org.dozer.DozerBeanMapper;
import org.dozer.Mapper;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.talentica.hungryHippos.tester.api.Service;
import com.talentica.hungryHippos.tester.api.ServiceError;
import com.talentica.hungryHippos.tester.api.job.JobServiceRequest;
import com.talentica.hungryHippos.tester.api.job.JobServiceResponse;
import com.talentica.hungryHippos.tester.web.UserCache;
import com.talentica.hungryHippos.tester.web.job.data.Job;
import com.talentica.hungryHippos.tester.web.job.data.JobInput;
import com.talentica.hungryHippos.tester.web.job.data.JobInputRepository;
import com.talentica.hungryHippos.tester.web.job.data.JobRepository;
import com.talentica.hungryHippos.tester.web.user.data.User;
import com.talentica.hungryHippos.utility.ScriptRunner;

@Controller
@RequestMapping("/job")
public class JobService extends Service {

	private Logger LOGGER = LoggerFactory.getLogger(JobService.class);
	
	private static final Mapper DOZER_BEAN_MAPPER = new DozerBeanMapper();

	@Value("${job.submission.script.path.jars.dir}")
	private String JOB_SUBMISSION_SCRIPT_FILE_PATH;

	@Autowired(required = false)
	private JobRepository jobRepository;

	@Autowired(required = false)
	private JobInputRepository jobInputRepository;

	@Autowired(required = false)
	private UserCache userCache;

	@RequestMapping(value = "new", method = RequestMethod.POST)
	public @ResponseBody JobServiceResponse newJob(@RequestBody(required = true) JobServiceRequest request) {
		JobServiceResponse jobServiceResponse = new JobServiceResponse();
		com.talentica.hungryHippos.tester.api.job.Job submittedJob = null;
		try {
			ServiceError error = request.validate();
			if (error != null) {
				jobServiceResponse.setError(error);
				cleanup(jobServiceResponse, submittedJob);
				return jobServiceResponse;
			}
			submittedJob = submitJob(request);
			jobServiceResponse.setJobDetail(submittedJob);
		} catch (Exception exception) {
			cleanup(jobServiceResponse, submittedJob);
			LOGGER.error("Error occurred while creating and submitting new job.-", exception);
			jobServiceResponse.setError(
					new ServiceError("Error occurred while processing your request. Please try after some time.",
							exception.getMessage()));
		}
		return jobServiceResponse;
	}

	private void cleanup(JobServiceResponse jobServiceResponse,
			com.talentica.hungryHippos.tester.api.job.Job submittedJob) {
		if (submittedJob != null && submittedJob.getJobId() != null) {
			if (submittedJob.getJobInput() != null && submittedJob.getJobInput().getJobInputId() != null) {
				jobInputRepository.delete(submittedJob.getJobInput().getJobInputId());
			}
			jobRepository.delete(submittedJob.getJobId());
			submittedJob.setUuid(null);
			if (jobServiceResponse.getError() == null) {
				jobServiceResponse.setError(new ServiceError("Job submission failed.", "No details available."));
			}
		}
	}

	private com.talentica.hungryHippos.tester.api.job.Job submitJob(JobServiceRequest request) {
		com.talentica.hungryHippos.tester.api.job.Job job = request.getJobDetail();
		com.talentica.hungryHippos.tester.api.job.JobInput jobInput = job.getJobInput();
		User user = userCache.getCurrentLoggedInUser();
		job.setUserId(user.getUserId());
		job.setDateTimeSubmitted(DateTime.now().toDate());
		job.setJobOutput(null);
		job.setJobInput(null);
		job.setStatus(com.talentica.hungryHippos.tester.api.job.STATUS.NOT_STARTED);
		Job jobEntity = jobRepository.save(DOZER_BEAN_MAPPER.map(job, Job.class));
		com.talentica.hungryHippos.tester.api.job.Job savedJob = DOZER_BEAN_MAPPER.map(jobEntity,
				com.talentica.hungryHippos.tester.api.job.Job.class);
		JobInput jobInputEntity = DOZER_BEAN_MAPPER.map(jobInput, JobInput.class);
		jobInputEntity.setJob(jobEntity);
		jobInputRepository.save(jobInputEntity);
		savedJob.setJobInput(jobInput);
		LOGGER.info("Executing job submission script.");
		String scriptExecutionOutput = ScriptRunner.executeShellScript(JOB_SUBMISSION_SCRIPT_FILE_PATH,
				savedJob.getUuid(), jobInputEntity.getJobMatrixClass());
		LOGGER.info("Job submission script executed successfully.");
		LOGGER.info("Script execution output for job:{} is {}",
				new Object[] { savedJob.getUuid(), scriptExecutionOutput });
		return savedJob;
	}

	@RequestMapping(value = "detail/{jobUuid}", method = RequestMethod.GET)
	public @ResponseBody JobServiceResponse getJobDetail(@PathVariable("jobUuid") String jobUuid) {
		JobServiceResponse jobServiceResponse = new JobServiceResponse();
		try {
			ServiceError error = validateUuid(jobUuid);
			if (error != null) {
				jobServiceResponse.setError(error);
				return jobServiceResponse;
			}
			User user = userCache.getCurrentLoggedInUser();
			Job job = jobRepository.findByUuidAndUserId(jobUuid, user.getUserId());
			if (job == null) {
				error = getInvalidJobUuidError(jobUuid);
				jobServiceResponse.setError(error);
				return jobServiceResponse;
			}
			jobServiceResponse
					.setJobDetail(DOZER_BEAN_MAPPER.map(job, com.talentica.hungryHippos.tester.api.job.Job.class));
		} catch (Exception exception) {
			LOGGER.error("Error occurred while getting job details.", exception);
			jobServiceResponse.setError(
					new ServiceError("Error occurred while processing your request. Please try after some time.",
							exception.getMessage()));
		}
		return jobServiceResponse;
	}
	
	// TODO: Allow only admins access to this REST API call.
	// @PreAuthorize(value="ADMIN")
	@RequestMapping(value = "any/detail/{jobUuid}", method = RequestMethod.GET)
	public @ResponseBody JobServiceResponse getAnyJobDetail(@PathVariable("jobUuid") String jobUuid) {
		JobServiceResponse jobServiceResponse = new JobServiceResponse();
		try {
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
			jobServiceResponse
					.setJobDetail(DOZER_BEAN_MAPPER.map(job, com.talentica.hungryHippos.tester.api.job.Job.class));
		} catch (Exception exception) {
			LOGGER.error("Error occurred while getting job details.", exception);
			jobServiceResponse.setError(
					new ServiceError("Error occurred while processing your request. Please try after some time.",
							exception.getMessage()));
		}
		return jobServiceResponse;
	}

	public void setJobInputRepository(JobInputRepository jobInputRepository) {
		this.jobInputRepository = jobInputRepository;
	}

	public void setJobRepository(JobRepository jobRepository) {
		this.jobRepository = jobRepository;
	}

	public void setUserCache(UserCache userCache) {
		this.userCache = userCache;
	}

}