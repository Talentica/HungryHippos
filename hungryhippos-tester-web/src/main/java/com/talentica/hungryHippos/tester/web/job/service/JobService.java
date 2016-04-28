package com.talentica.hungryHippos.tester.web.job.service;

import java.io.File;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import com.talentica.hungryHippos.utility.SecureShellExecutor;

@Controller
@RequestMapping("/job")
public class JobService extends Service {

	private Logger LOGGER = LoggerFactory.getLogger(JobService.class);

	private static final Mapper DOZER_BEAN_MAPPER = new DozerBeanMapper();

	private static final String SPACE = " ";

	@Value("${job.submission.script.execution.command}")
	private String JOB_SUBMISSION_SCRIPT_EXECUTION_COMMAND;

	@Value("${job.submission.script.dir}")
	private String JOB_SUBMISSION_SCRIPT_DIRECTORY;

	@Value("${job.submission.script.host}")
	private String JOB_SUBMISSION_SCRIPT_HOST;

	@Value("${job.submission.script.host.username}")
	private String JOB_SUBMISSION_SCRIPT_HOST_USERNAME;

	@Value("${job.submission.script.host.private.key.file.path}")
	private String JOB_SUBMISSION_SCRIPT_HOST_PRIVATE_KEY_FILE_PATH;

	@Value("${job.submission.script.host.password}")
	private String JOB_SUBMISSION_SCRIPT_HOST_PASSWORD;

	@Value("${job.submission.script.execution.log.dir}")
	private String JOB_SUBMISSION_SCRIPT_EXECUTION_LOG_DIRECTORY;

	@Value("${job.submission.script.change.dir.command}")
	private String CHANGE_DIRECTORY_COMMAND;

	@Value("${job.submission.script.commands.separator}")
	private String COMMANDS_SEPARATOR;

	@Value("${spring.datasource.username}")
	private String DB_CONN_USERNAME;

	@Value("${spring.datasource.password}")
	private String DB_CONN_PASSWORD;

	@Autowired(required = false)
	private JobRepository jobRepository;

	@Value("${spring.datasource.url}")
	private String DB_CONN_URL;

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

	private com.talentica.hungryHippos.tester.api.job.Job submitJob(JobServiceRequest request)
			throws InterruptedException {
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
		executeJobSubmissionScript(savedJob, jobInputEntity);
		return savedJob;
	}

	private void executeJobSubmissionScript(com.talentica.hungryHippos.tester.api.job.Job savedJob,
			JobInput jobInputEntity) {
		LOGGER.info("Executing job submission script.");
		SecureShellExecutor secureShellExecutor = new SecureShellExecutor(JOB_SUBMISSION_SCRIPT_HOST,
				JOB_SUBMISSION_SCRIPT_HOST_USERNAME, JOB_SUBMISSION_SCRIPT_HOST_PRIVATE_KEY_FILE_PATH,
				JOB_SUBMISSION_SCRIPT_HOST_PASSWORD);
		String uuid = savedJob.getUuid();
		String scriptLogFile = JOB_SUBMISSION_SCRIPT_EXECUTION_LOG_DIRECTORY + File.separator + uuid + File.separator
				+ "jobsubmission.script.out";
		String changeDirCommand = CHANGE_DIRECTORY_COMMAND + SPACE + JOB_SUBMISSION_SCRIPT_DIRECTORY;
		String dbConnectionParameters = getDatabaseHost(DB_CONN_URL) + SPACE + DB_CONN_USERNAME + SPACE
				+ DB_CONN_PASSWORD;
		String scriptExecutionCommand = JOB_SUBMISSION_SCRIPT_EXECUTION_COMMAND + SPACE + uuid + SPACE
				+ jobInputEntity.getJobMatrixClass() + SPACE + dbConnectionParameters + "> " + scriptLogFile + SPACE
				+ "2> " + scriptLogFile + " & ";
		String command = changeDirCommand + COMMANDS_SEPARATOR + scriptExecutionCommand;
		LOGGER.info("Script being executed:{}", command);
		List<String> scriptExecutionOutput = secureShellExecutor.execute(command);
		LOGGER.info("Job submission script executed successfully.");
		LOGGER.info("Script execution output for job:{} is {}", new Object[] { uuid, scriptExecutionOutput });
	}

	private static String getDatabaseHost(String dbConnectionUrl) {
		String dbHost = null;
		Pattern dbConnectionUrlPattern = Pattern.compile("(.*)(//)(.*)(:)(.*)");
		Matcher matcher = dbConnectionUrlPattern.matcher(dbConnectionUrl);
		if (matcher.find()) {
			dbHost = matcher.group(3);
		}
		return dbHost;
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