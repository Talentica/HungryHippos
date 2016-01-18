/**
 * 
 */
package com.talentica.hungryHippos.accumulator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author PooshanS
 *
 */
public class JobEntity implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Job job;
	private Map<String,Integer> workerIdRowCountMap;
	public JobEntity(Job job){
		this.job = job;
	}
	public Job getJob() {
		return job;
	}
	public void setJob(Job job) {
		this.job = job;
	}
	public Map<String, Integer> getWorkerIdRowCountMap() {
		if(workerIdRowCountMap == null){
			workerIdRowCountMap = new HashMap<String, Integer>();
		}
		return workerIdRowCountMap;
	}
	public void setWorkerIdRowCountMap(Map<String, Integer> workerIdRowCountMap) {
		this.workerIdRowCountMap = workerIdRowCountMap;
	}
	
	
	
}
