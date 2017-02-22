/**
 * 
 */
package com.talentica.hungryHippos.rdd.listener;

import org.apache.spark.TaskContext;
import org.apache.spark.util.TaskFailureListener;

import com.talentica.hungryHippos.rdd.HHRDDPartition;

/**
 * @author pooshans
 *
 */
public class TaskFailure implements TaskFailureListener {

  private HHRDDPartition partition;

  public TaskFailure(HHRDDPartition partition) {
    this.partition = partition;
  }

  @Override
  public void onTaskFailure(TaskContext taskContext, Throwable arg1) {
    
  }

}
