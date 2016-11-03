package com.talentica.hungryHippos.master.job;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.JobConfigPublisher;
import com.talentica.hungryHippos.JobStatusClientCoordinator;
import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.coordination.utility.ZkSignalListener;
import com.talentica.hungryHippos.utility.JobEntity;

/**
 * {@code JobManager} Manages the Job.
 */
public class JobManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobManager.class);
  private List<JobEntity> jobEntities = new ArrayList<JobEntity>();

  /**
   * adds a list of job.
   * 
   * @param jobList
   */
  public void addJobList(List<Job> jobList) {
    JobEntity jobEntity;
    for (Job job : jobList) {
      jobEntity = new JobEntity();
      jobEntity.setJob(job);
      this.jobEntities.add(jobEntity);
    }
  }

  /**
   * To start the job manager.
   * 
   * @throws Exception
   */
  public void start(String jobUUId) throws Exception {
    JobConfigPublisher.uploadJobEntities(jobUUId, jobEntities);
    LOGGER.info("SEND TASKS TO NODES");
    JobStatusClientCoordinator.initializeJobNodes(jobUUId);
    LOGGER.info("SIGNAL IS SENT TO ALL NODES TO START JOB MATRIX");
    boolean areNodesCompleted = checkNodesStatusSignal();
    if (areNodesCompleted) {
      JobStatusClientCoordinator.updateJobCompleted(jobUUId);
    } else {
      JobStatusClientCoordinator.updateJobFailed(jobUUId);
    }
    LOGGER.info("\n\n\n\t FINISHED!\n\n\n");
  }


  /**
   * Waits and checks the outcome of the Nodes
   * 
   * @return
   */
  private boolean checkNodesStatusSignal() {
    boolean areNodesCompleted = false;
    boolean hasAnyNodeFailed = false;
    while (!(areNodesCompleted || hasAnyNodeFailed)) {
      areNodesCompleted =
          JobStatusClientCoordinator.areAllNodesCompleted(ZkSignalListener.jobuuidInBase64);
      hasAnyNodeFailed =
          JobStatusClientCoordinator.hasAnyNodeFailed(ZkSignalListener.jobuuidInBase64);
    }
    LOGGER.info("ALL NODES FINISHED THE JOBS");
    return areNodesCompleted;

  }



}
