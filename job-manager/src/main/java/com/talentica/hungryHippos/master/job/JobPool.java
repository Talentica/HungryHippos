/**
 * 
 */
package com.talentica.hungryHippos.master.job;

import java.util.Queue;

import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.utility.JobEntity;

/**
 * {@code JobPool} used for keeping track of jobs.
 * 
 * @author PooshanS
 *
 */
public interface JobPool {

  enum status {
    ACTIVE, INACTIVE, CANCEL, SUCCEEDED, STOPPED, ERROR, POOLED;
  }

  /**
   * adds a jobEntity.
   * 
   * @param jobEntity
   */
  void addJobEntity(JobEntity jobEntity);

  /**
   * removes a jobEntity.
   * 
   * @param jobEntity
   */
  void removeJobEntity(JobEntity jobEntity);

  /**
   * checks whether the job is empty.
   * 
   * @return
   */
  boolean isEmpty();

  /**
   * number of jobs.
   * 
   * @return
   */
  int size();

  /**
   * returns a queue contains all the jobEntity.
   * 
   * @return
   */
  Queue<JobEntity> getQueue();

  /**
   * removes a job from the queue.
   * 
   * @return
   */
  JobEntity pollJobEntity();

  /**
   * peeks a the first JobEntity in the queue. It doesn't remove that JobEntity from the queue.
   * 
   * @return
   */
  JobEntity peekJobEntity();

}
