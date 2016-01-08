/**
 * 
 */
package com.talentica.hungryHippos.accumulator;

import java.io.Serializable;

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
	private int rowCount = 0;
	public JobEntity(Job job){
		this.job = job;
	}
	public Job getJob() {
		return job;
	}
	public void setJob(Job job) {
		this.job = job;
	}
	public int getRowCount() {
		return rowCount;
	}
	
	public void incrRowCount(){
		rowCount++;
	}
}
