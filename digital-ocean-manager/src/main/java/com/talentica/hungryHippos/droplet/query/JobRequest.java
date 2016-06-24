/**
 * 
 */
package com.talentica.hungryHippos.droplet.query;

import java.io.IOException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.GetMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.tester.api.job.Job;
import com.talentica.hungryHippos.tester.api.job.JobServiceResponse;

/**
 * @author PooshanS
 *
 */
public class JobRequest {

	private static final Logger LOGGER = LoggerFactory.getLogger(JobRequest.class);

	private static final ObjectMapper MAPPER = new ObjectMapper();

	public Job getJobDetails(String uuid) throws HttpException, IOException {
		final String WEBSERVER_IP = CoordinationApplicationContext.getProperty().getValueByKey("common.webserver.ip");
		final String WEBSERVER_PORT = CoordinationApplicationContext.getProperty().getValueByKey("common.webserver.port");
		final String WEBSERVER_IP_PORT = WEBSERVER_IP + ":" + WEBSERVER_PORT;
		String uri = "http://" + WEBSERVER_IP_PORT + "/job/any/detail/" + uuid;
		LOGGER.info("Getting details of job:{}", uri);
		GetMethod getJobDetails = new GetMethod(uri);
		HttpClient httpClient = new HttpClient();
		httpClient.executeMethod(getJobDetails);
		JobServiceResponse jobServiceResponse = MAPPER.readValue(getJobDetails.getResponseBody(),
				JobServiceResponse.class);
		return jobServiceResponse.getJobDetail();
	}
}
