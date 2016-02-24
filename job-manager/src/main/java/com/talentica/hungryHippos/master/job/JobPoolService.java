/**
 * 
 */
package com.talentica.hungryHippos.master.job;

import java.util.LinkedList;
import java.util.Queue;

import com.talentica.hungryHippos.utility.JobEntity;


/**
 * JobPooling is used to schedule the job based on availability of the resources. Job is pooled according to the Job's memory requirement.
 * 
 * @author PooshanS
 *
 */
public class JobPoolService implements JobPool{

	private Queue<JobEntity> jobPriorityQueue;
	
	public JobPoolService() {
		jobPriorityQueue = new LinkedList<JobEntity>();
	}
	
	@Override
	public void addJobEntity(JobEntity jobEntity) {
		jobPriorityQueue.add(jobEntity);		
	}

	@Override
	public void removeJobEntity(JobEntity jobEntity) {
		jobPriorityQueue.remove(jobEntity);
		
	}

	@Override
	public boolean isEmpty() {
		return jobPriorityQueue.isEmpty();
	}

	@Override
	public int size() {
		return jobPriorityQueue.size();
	}
	
	@Override
	public Queue<JobEntity> getQueue() {
		return jobPriorityQueue;
	}

	@Override
	public JobEntity pollJobEntity() {
		return jobPriorityQueue.poll();
	}

	@Override
	public JobEntity peekJobEntity() {
		return jobPriorityQueue.peek();
	}

}
