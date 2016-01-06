/**
 * 
 */
package com.talentica.hungryHippos.manager.job;

import java.util.Queue;

import com.talentica.hungryHippos.accumulator.Job;

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
