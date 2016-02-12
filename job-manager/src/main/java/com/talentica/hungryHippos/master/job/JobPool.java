/**
 * 
 */
package com.talentica.hungryHippos.master.job;

import java.util.Queue;

import com.talentica.hungryHippos.client.job.Job;

/**
 * @author PooshanS
 *
 */
public interface JobPool {
	
	enum status{
		ACTIVE,INACTIVE,CANCEL,SUCCEEDED,STOPPED,ERROR,POOLED;
	}
	
	void addJob(Job job);
	
	void removeJob(Job job);
	
	boolean isEmpty();
	
	int size();
	
	Queue<Job> getQueue();
	
	Job pollJob();
	
	Job peekJob();

}
