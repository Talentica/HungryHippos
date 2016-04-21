/**
 * 
 */
package com.talentica.hungryHippos.droplet.query;

import java.io.IOException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.GetMethod;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.talentica.hungryHippos.coordination.utility.Property;
import com.talentica.hungryHippos.tester.api.job.Job;
import com.talentica.hungryHippos.tester.api.job.JobServiceResponse;

/**
 * @author PooshanS
 *
 */
public class JobRequest {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	public Job getJobDetails(String uuid) throws HttpException, IOException {
			final String WEBSERVER_IP = Property.getProperties().getProperty(
					"common.webserver.ip");
			final String WEBSERVER_PORT = Property.getProperties().getProperty("common.webserver.port");
			final String WEBSERVER_IP_PORT = WEBSERVER_IP + ":" + WEBSERVER_PORT;
		GetMethod getJobDetails = new GetMethod("http://" + WEBSERVER_IP_PORT
					+ "/job/any/detail/" + uuid);
		HttpClient httpClient = new HttpClient();
		httpClient.executeMethod(getJobDetails);
		JobServiceResponse jobServiceResponse = MAPPER.readValue(getJobDetails.getResponseBody(),
				JobServiceResponse.class);
		return jobServiceResponse.getJobDetail();
	}
}
