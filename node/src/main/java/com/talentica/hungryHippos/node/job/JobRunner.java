package com.talentica.hungryHippos.node.job;

import java.io.Serializable;
import java.util.List;

import com.talentica.hungryHippos.common.job.PrimaryDimensionwiseJobsCollection;

public interface JobRunner extends Serializable {

  public void run(String jobUuid, List<PrimaryDimensionwiseJobsCollection> jobsCollectionList);

}
