/**
 * 
 */
package com.talentica.hungryHippos.manager.job;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

import com.talentica.hungryHippos.accumulator.JobEntity;

/**
 * JobPooling is used to schedule the job based on availability of the resources. Job is pooled according to the Job's memory requirement.
 * 
 * @author PooshanS
 *
 */
public class JobPoolService implements JobPool{

	private Queue<JobEntity> jobPriorityQueue;
	
	private static int DEFAULT_POOL_SIZE_CAPACITY = 11;
	
	public JobPoolService() {
		jobPriorityQueue = new PriorityQueue<>(DEFAULT_POOL_SIZE_CAPACITY,sizeComparator);
	}
	
	public JobPoolService(int poolCapacity){
		jobPriorityQueue = new PriorityQueue<>(poolCapacity,sizeComparator);
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
	
	public static Comparator<JobEntity> sizeComparator = new Comparator<JobEntity>(){        
        @Override
        public int compare(JobEntity jobEntity1, JobEntity jobEntity2) {
            return (int) (jobEntity1.getRowCount() - jobEntity2.getRowCount());
        }
    };

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
