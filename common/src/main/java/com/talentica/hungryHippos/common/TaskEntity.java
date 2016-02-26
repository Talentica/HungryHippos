/**
 * 
 */
package com.talentica.hungryHippos.common;

import java.io.Serializable;

import com.talentica.hungryHippos.client.domain.ValueSet;
import com.talentica.hungryHippos.client.domain.Work;
import com.talentica.hungryHippos.utility.JobEntity;

/**
 * @author PooshanS
 *
 */
public class TaskEntity implements Serializable,Cloneable {
	private static final long serialVersionUID = 1L;
	private int taskId = 0;
	private static int counter = 0;
	private JobEntity jobEntity;
	private Work work;
	private int rowCount;
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
	public int getRowCount() {
		return rowCount;
	}
	
	public JobEntity getJobEntity() {
		return jobEntity;
	}
	public void setJobEntity(JobEntity jobEntity) {
		this.jobEntity = jobEntity;
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
	
	public void setRowCount(int rowCount){
		this.rowCount = rowCount;
	}
	
	public Object clone()throws CloneNotSupportedException{  
	      return (TaskEntity)super.clone();  
	   }
}

