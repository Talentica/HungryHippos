package com.talentica.hungryHippos.tester.web.job.service;

import com.talentica.hungryHippos.tester.web.service.ServiceResponse;

import lombok.Getter;
import lombok.Setter;

public class JobJarFileUploadServiceResponse extends ServiceResponse {

	@Getter
	@Setter
	private long uploadedFileSize = 0;

	@Getter
	@Setter
	private String jobUuid;

}
