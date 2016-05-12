package com.talentica.hungryHippos.tester.web.job.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/env")
public class EnvironmentService {

	@Value("${client.api.download.url}")
	private String CLIENT_API_DOWNLOAD_URL;

	@Value("${sample.test.jobs.jar.file.url}")
	private String SAMPLE_TEST_JOBS_JAR_FILE_URL;

	@Value("${client.api.docs.url}")
	private String CLIENT_API_DOCS_URL;

	@RequestMapping(method = RequestMethod.GET, value = "clientapidownloadlocation")
	public @ResponseBody EnvironmentServiceResponse getClientApiDownloadLocation() {
		return new EnvironmentServiceResponse(CLIENT_API_DOWNLOAD_URL);
	}

	@RequestMapping(method = RequestMethod.GET, value = "testjobsjarfilelocation")
	public @ResponseBody EnvironmentServiceResponse getSampleTestJobsJarFileLocation() {
		return new EnvironmentServiceResponse(SAMPLE_TEST_JOBS_JAR_FILE_URL);
	}

	@RequestMapping(method = RequestMethod.GET, value = "clientapidocslocation")
	public @ResponseBody EnvironmentServiceResponse getClientApiDocsLocation() {
		return new EnvironmentServiceResponse(CLIENT_API_DOCS_URL);
	}

}