/**
 * 
 */
package com.talentica.hungryHippos.common;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.utility.MapUtils;

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
	
	@Override
	public String toString() {
		if (job != null && workerIdRowCountMap != null) {
			return "Job Id: " + job.getJobId() + "" + MapUtils.getFormattedString(workerIdRowCountMap);
		}
		return super.toString();
	}
	
	
}
