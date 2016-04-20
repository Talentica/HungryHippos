/**
 * 
 */
package com.talentica.hungryHippos.droplet.query;

import java.io.BufferedInputStream;
import java.io.InputStream;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.talentica.hungryHippos.coordination.utility.Property;
import com.talentica.hungryHippos.tester.web.job.data.Job;
import com.talentica.hungryHippos.tester.web.job.service.JobServiceResponse;

/**
 * @author PooshanS
 *
 */
@Component
public class JobRequest {

	@SuppressWarnings("deprecation")
	HttpClient httpClient = new DefaultHttpClient();

	public Job getJobDetails(String uuid) {
		try {
			final String WEBSERVER_IP = Property.getProperties().getProperty(
					"common.webserver.ip");
			HttpGet httpGetRequest = new HttpGet("http://" + WEBSERVER_IP
					+ "/job/any/detail/" + uuid);
			HttpResponse httpResponse = httpClient.execute(httpGetRequest);
			HttpEntity entity = httpResponse.getEntity();
			byte[] buffer = new byte[1024];
			if (entity != null) {
				InputStream inputStream = entity.getContent();
				try {
					int bytesRead = 0;
					BufferedInputStream bis = new BufferedInputStream(
							inputStream);
					ObjectMapper mapper = new ObjectMapper();
					StringBuffer stringBuffer = new StringBuffer();
					while ((bytesRead = bis.read(buffer)) != -1) {
						String chunk = new String(buffer, 0, bytesRead);
						stringBuffer.append(chunk);
					}
					JobServiceResponse jobServiceResponse = mapper.readValue(
							stringBuffer.toString(), JobServiceResponse.class);
					if (jobServiceResponse != null){
						return jobServiceResponse.getJobDetail();
					}
					else{
					  return null;	
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					try {
						inputStream.close();
					} catch (Exception ignore) {
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			httpClient.getConnectionManager().shutdown();
		}
		return null;
	}
}
