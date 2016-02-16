/**
 * 
 */
package com.talentica.hungryHippos.utility;

import java.io.Serializable;

import com.talentica.hungryHippos.client.job.Job;

/**
 * @author PooshanS
 *
 */
public class JobEntity implements Serializable {
	private static final long serialVersionUID = 7062343992924390450L;
	private int jobId;
	private static int counter;
	private Job job;
	
	public JobEntity(){
		jobId = counter++;
	}
	public int getJobId() {
		return jobId;
	}
	
	public Job getJob() {
		return job;
	}
	public void setJob(Job job) {
		this.job = job;
	}
	
	@Override
	public boolean equals(Object object) {
		boolean result = false;
		if (object == null || object.getClass() != getClass()) {
			result = false;
		} else {
			JobEntity jobEntity = (JobEntity) object;
			if (this.jobId == jobEntity.getJobId()) {
				result = true;
			}
		}
		return result;
	}

	@Override
	public int hashCode() {
		return this.jobId;
	}

}
