package com.talentica.hungryHippos.tester.web.job.service;

import java.io.IOException;
import java.math.BigDecimal;

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
					"Missing job information in request.");
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

		String dataLocation = jobDetail.getJobInput().getDataLocation();
		try {
			HeadMethod head = new HeadMethod(dataLocation);
			HttpClient httpClient = new HttpClient();
			httpClient.executeMethod(head);
			long dataSize = head.getResponseContentLength();
			if (dataSize <= 0 || head.getStatusCode() == 404) {
				error = new ServiceError(
						"Either input data file content is empty or file does not exist. Please provide valid input data file.",
						"Empty data file.");
			}
			jobDetail.getJobInput().setDataSize(BigDecimal.valueOf(Double.valueOf(dataSize) / 1024));
		} catch (Exception exception) {
			error = new ServiceError(
					"Could not get size of input CSV file from location:" + dataLocation
							+ ". Please make sure file is accessible over internet for processing.",
					"Invalid data file location.");
		}
		return error;
	}

}