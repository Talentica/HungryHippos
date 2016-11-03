/**
 * 
 */
package com.talentica.hungryHippos.master.job;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;

import com.talentica.hungryHippos.coordination.domain.LeafBean;
import com.talentica.hungryHippos.utility.JobEntity;

/**
 * {@code NodesJobsRunnable} used for running jobs on Nodes.
 * 
 * @author PooshanS
 *
 */
public interface NodesJobsRunnable {

  /**
   * creates Job service for each node.
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   * @throws ClassNotFoundException
   */
  void createNodeJobService()
      throws IOException, InterruptedException, KeeperException, ClassNotFoundException;

  /**
   * schedules a task manager.
   * 
   * @param jobUUId
   * @throws InterruptedException
   * @throws KeeperException
   * @throws ClassNotFoundException
   * @throws IOException
   */
  void scheduleTaskManager(String jobUUId)
      throws InterruptedException, KeeperException, ClassNotFoundException, IOException;

  /**
   * adds a JobEntity.
   * 
   * @param jobEntity
   */
  public void addJob(JobEntity jobEntity);

  /**
   * adds a List of JobEntity.
   * 
   * @param jobEntities
   */
  void addJobs(List<JobEntity> jobEntities);

  /**
   * retrieves the taskmanager associated.
   * 
   * @return
   */
  TaskManager getTaskManager();

  /**
   * sends notification to run all jobs.
   * 
   * @param jobEntity
   * @param signal
   * @param jobUUId
   * @return
   * @throws InterruptedException
   * @throws KeeperException
   * @throws ClassNotFoundException
   * @throws IOException
   */
  boolean sendJobRunnableNotificationToNode(JobEntity jobEntity, CountDownLatch signal,
      String jobUUId)
      throws InterruptedException, KeeperException, ClassNotFoundException, IOException;

  /**
   * sets the status of job.
   * 
   * @return
   * @throws InterruptedException
   * @throws KeeperException
   * @throws ClassNotFoundException
   * @throws IOException
   */
  Set<LeafBean> receiveJobSucceedNotificationFromNode()
      throws InterruptedException, KeeperException, ClassNotFoundException, IOException;
}
