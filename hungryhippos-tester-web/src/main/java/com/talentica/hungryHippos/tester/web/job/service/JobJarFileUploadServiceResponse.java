package com.talentica.hungryHippos.tester.web.job.service;

import com.talentica.hungryHippos.tester.api.ServiceResponse;

public class JobJarFileUploadServiceResponse extends ServiceResponse {

	private long uploadedFileSize = 0;

	private String jobUuid;

	public long getUploadedFileSize() {
		return uploadedFileSize;
	}

	public void setUploadedFileSize(long uploadedFileSize) {
		this.uploadedFileSize = uploadedFileSize;
	}

	public String getJobUuid() {
		return jobUuid;
	}

	public void setJobUuid(String jobUuid) {
		this.jobUuid = jobUuid;
	}

}
