/**
 * 
 */
package com.talentica.hungryHippos.manager.job;

import java.util.Queue;

import com.talentica.hungryHippos.accumulator.JobEntity;

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
