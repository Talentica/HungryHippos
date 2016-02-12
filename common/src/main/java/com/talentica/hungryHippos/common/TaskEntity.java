/**
 * 
 */
package com.talentica.hungryHippos.common;

import java.io.Serializable;

import com.talentica.hungryHippos.client.domain.ValueSet;
import com.talentica.hungryHippos.client.domain.Work;
import com.talentica.hungryHippos.client.job.Job;

/**
 * @author PooshanS
 *
 */
public class TaskEntity implements Serializable {
	private static final long serialVersionUID = 1L;
	private int taskId = 0;
	private static int counter = 0;
	private Job job;
	private Work work;
	private Integer rowCount;
	private ValueSet valueSet;
	
	public TaskEntity(){
		taskId = counter++;
		rowCount = 0;
	}
	public Work getWork() {
		return work;
	}
	public void setWork(Work work) {
		this.work = work;
	}
	public Integer getRowCount() {
		return rowCount;
	}
	
	public Job getJob() {
		return job;
	}
	public void setJob(Job job) {
		this.job = job;
	}

	public int getTaskId() {
		return taskId;
	}

	public ValueSet getValueSet() {
		return valueSet;
	}

	public void setValueSet(ValueSet valueSet) {
		this.valueSet = valueSet;
	}

	public void incrRowCount(){
		rowCount++;
	}
}

