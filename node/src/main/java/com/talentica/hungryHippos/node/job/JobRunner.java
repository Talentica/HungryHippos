package com.talentica.hungryHippos.node.job;

import java.io.Serializable;
import java.util.List;

import com.talentica.hungryHippos.common.job.PrimaryDimensionwiseJobsCollection;

/**
 * 
 * {@code JobRunner} , used for running the jobs in the system.
 *
 */
public interface JobRunner extends Serializable {

  /**
   * runs the job.
   * 
   * @param jobUuid
   * @param jobsCollectionList
   */
  public void run(String jobUuid, List<PrimaryDimensionwiseJobsCollection> jobsCollectionList);

}
