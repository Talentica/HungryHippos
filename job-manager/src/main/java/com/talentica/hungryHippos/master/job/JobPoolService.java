/**
 * 
 */
package com.talentica.hungryHippos.master.job;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

import com.talentica.hungryHippos.client.job.Job;


/**
 * JobPooling is used to schedule the job based on availability of the resources. Job is pooled according to the Job's memory requirement.
 * 
 * @author PooshanS
 *
 */
public class JobPoolService implements JobPool{

	private Queue<Job> jobPriorityQueue;
	
	private static int DEFAULT_POOL_SIZE_CAPACITY = 11;
	
	public JobPoolService() {
		jobPriorityQueue = new PriorityQueue<>(DEFAULT_POOL_SIZE_CAPACITY,sizeComparator);
	}
	
	public JobPoolService(int poolCapacity){
		jobPriorityQueue = new PriorityQueue<>(poolCapacity,sizeComparator);
	}
	
	@Override
	public void addJob(Job job) {
		jobPriorityQueue.add(job);		
	}

	@Override
	public void removeJob(Job job) {
		jobPriorityQueue.remove(job);
		
	}

	@Override
	public boolean isEmpty() {
		return jobPriorityQueue.isEmpty();
	}

	@Override
	public int size() {
		return jobPriorityQueue.size();
	}
	
	public static Comparator<Job> sizeComparator = new Comparator<Job>(){        
        @Override
        public int compare(Job job1, Job job2) {
            return (int) (job1.getJobId() - job2.getJobId());
        }
    };

	@Override
	public Queue<Job> getQueue() {
		return jobPriorityQueue;
	}

	@Override
	public Job pollJob() {
		return jobPriorityQueue.poll();
	}

	@Override
	public Job peekJob() {
		return jobPriorityQueue.peek();
	}

}
