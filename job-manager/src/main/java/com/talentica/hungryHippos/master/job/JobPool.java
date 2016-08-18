/**
 * 
 */
package com.talentica.hungryHippos.master.job;

import java.util.Queue;

import com.talentica.hungryHippos.utility.JobEntity;

/**
 * @author PooshanS
 *
 */
public interface JobPool {
	
	enum status{
		ACTIVE,INACTIVE,CANCEL,SUCCEEDED,STOPPED,ERROR,POOLED;
	}
	
	void addJobEntity(JobEntity jobEntity);
	
	void removeJobEntity(JobEntity jobEntity);
	
	boolean isEmpty();
	
	int size();
	
	Queue<JobEntity> getQueue();
	
	JobEntity pollJobEntity();
	
	JobEntity peekJobEntity();

}
