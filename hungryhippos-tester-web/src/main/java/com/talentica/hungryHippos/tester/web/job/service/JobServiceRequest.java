package com.talentica.hungryHippos.tester.web.job.service;

import java.io.IOException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.lang3.StringUtils;

import com.talentica.hungryHippos.tester.web.job.data.Job;
import com.talentica.hungryHippos.tester.web.service.ServiceError;

import lombok.Getter;
import lombok.Setter;

public class JobServiceRequest {

	@Getter
	@Setter
	private Job jobDetail;

	public ServiceError validate() throws HttpException, IOException {
		ServiceError error = null;
		if (jobDetail == null || jobDetail == null || jobDetail.getJobInput() == null
				|| StringUtils.isBlank(jobDetail.getJobInput().getDataLocation())) {
			error = new ServiceError("Job information is blank. Please provide with the job information.",
					"Missing jon information in request.");
		}

		if (jobDetail != null && jobDetail.getJobId() != null) {
			error = new ServiceError("Please provide with valid job information.",
					"Job id cannot already set in new job creation request.");
		}

		if (jobDetail != null && StringUtils.isBlank(jobDetail.getUuid())) {
			error = new ServiceError(
					"Please provide with valid job UUID in request. You should get it after successful upload of a valid job jar file.",
					"Job UUID not found.");
		}

		HeadMethod head = new HeadMethod(jobDetail.getJobInput().getDataLocation());
		HttpClient httpClient = new HttpClient();
		httpClient.executeMethod(head);
		long dataSize = head.getResponseContentLength();
		if (dataSize == 0) {
			error = new ServiceError("Input data file is empty. Please provide input data file with some content.",
					"Empty data file.");
		}
		jobDetail.getJobInput().setDataSize((int) dataSize);
		return error;
	}

}
