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
public class TaskEntity implements Serializable, Cloneable {
  private static final long serialVersionUID = 1L;
  private int taskId = 0;
  private static int counter = 0;
  private JobEntity jobEntity;
  private Work work;
  private long rowCount;
  private ValueSet valueSet;

  /**
   * creates a new TaskEntity.
   */
  public TaskEntity() {
    taskId = counter++;
    rowCount = 0;
  }

  /**
   * 
   * @return a Work associated with this task.
   */
  public Work getWork() {
    return work;
  }

  /**
   * sets a work associated with this task.
   * 
   * @param work
   */
  public void setWork(Work work) {
    this.work = work;
  }

  /**
   * 
   * @return a long value representing the row count for this task.
   */
  public long getRowCount() {
    return rowCount;
  }

  /**
   * 
   * @return an instance of JobEntity associated with this {@code Task.}
   */
  public JobEntity getJobEntity() {
    return jobEntity;
  }

  /**
   * sets a JobEntity to this task.
   * 
   * @param jobEntity
   */
  public void setJobEntity(JobEntity jobEntity) {
    this.jobEntity = jobEntity;
  }

  /**
   * 
   * @return a int value represnting this tasks id.
   */
  public int getTaskId() {
    return taskId;
  }

  /**
   * 
   * @return an instance of ValueSet associated with this task.
   */
  public ValueSet getValueSet() {
    return valueSet;
  }

  /**
   * sets a ValueSet to this task.
   * 
   * @param valueSet
   */
  public void setValueSet(ValueSet valueSet) {
    this.valueSet = valueSet;
  }

  /**
   * increments the row count associated with this task.
   */
  public void incrRowCount() {
    rowCount++;
  }

  /**
   * sets the row count associated with this task.
   * 
   * @param rowCount
   */
  public void setRowCount(long rowCount) {
    this.rowCount = rowCount;
  }

  @Override
  public TaskEntity clone() throws CloneNotSupportedException {
    TaskEntity taskEntity = new TaskEntity();
    taskEntity.setRowCount(rowCount);
    taskEntity.setValueSet(valueSet);
    return taskEntity;
  }
}
