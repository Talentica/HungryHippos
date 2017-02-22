/**
 * 
 */
package com.talentica.hungryHippos.rdd;

import java.util.Properties;

import org.apache.spark.TaskContext;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.Task;

/**
 * @author pooshans
 *
 */
public class HHTask<T> extends Task<T> {

  private static final long serialVersionUID = 7588730450784412699L;

  public HHTask(int stageId, int stageAttemptId, int partitionId, TaskMetrics metrics,
      Properties localProperties) {
    
    super(stageId, stageAttemptId, partitionId, metrics, localProperties);

  }

  @Override
  public T runTask(TaskContext taskContext) {
    return null;
  }

}
