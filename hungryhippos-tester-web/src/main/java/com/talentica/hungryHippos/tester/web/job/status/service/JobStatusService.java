package com.talentica.hungryHippos.tester.web.job.status.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.talentica.hungryHippos.tester.web.job.data.Job;
import com.talentica.hungryHippos.tester.web.job.data.JobRepository;
import com.talentica.hungryHippos.tester.web.job.output.data.JobOutputRepository;
import com.talentica.hungryHippos.tester.web.job.service.JobDetail;
import com.talentica.hungryHippos.tester.web.job.status.data.ProcessInstance;
import com.talentica.hungryHippos.tester.web.job.status.data.ProcessInstanceRepository;
import com.talentica.hungryHippos.tester.web.service.Service;
import com.talentica.hungryHippos.tester.web.service.ServiceError;

import lombok.Setter;

@Controller
@RequestMapping("/job")
public class JobStatusService extends Service {

	@Setter
	@Autowired(required = false)
	private JobRepository jobRepository;

	@Setter
	@Autowired(required = false)
	private ProcessInstanceRepository processInstanceRepository;

	@Setter
	@Autowired(required = false)
	private JobOutputRepository jobOutputRepository;

	@RequestMapping(value = "status/{jobUuid}", method = RequestMethod.GET)
	public @ResponseBody JobStatusServiceResponse getJobStatus(@PathVariable("jobUuid") String jobUuid) {
		JobStatusServiceResponse jobStatusServiceResponse = new JobStatusServiceResponse();
		ServiceError error = validateUuid(jobUuid);
		if (error != null) {
			jobStatusServiceResponse.setError(error);
			return jobStatusServiceResponse;
		}
		Job job = jobRepository.findByUuid(jobUuid);
		if (job == null) {
			error = getInvalidJobUuidError(jobUuid);
			jobStatusServiceResponse.setError(error);
			return jobStatusServiceResponse;
		}
		Integer jobId = job.getJobId();
		List<ProcessInstance> processInstances = processInstanceRepository.findByJobId(jobId);
		JobDetail jobDetail = new JobDetail();
		jobDetail.setJob(job);
		jobDetail.setJobOutput(jobOutputRepository.findByJobId(jobId));
		jobStatusServiceResponse.setJobDetail(jobDetail);
		jobStatusServiceResponse.setProcessInstances(processInstances);
		return jobStatusServiceResponse;
	}

}