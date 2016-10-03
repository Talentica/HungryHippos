package com.talentica.hungryHippos.node.job;

import java.io.File;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.common.DataRowProcessor;
import com.talentica.hungryHippos.common.job.PrimaryDimensionwiseJobsCollection;
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.RowProcessor;
import com.talentica.hungryHippos.storage.StoreAccess;
import com.talentica.hungryHippos.utility.JobEntity;
import com.talentica.hungryHippos.utility.MemoryStatus;
import com.talentica.hungryhippos.filesystem.HungryHipposFileSystem;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;

public class UnsortedDataJobRunner implements JobRunner {

  private static final long serialVersionUID = -4793614653018059851L;

  private DataStore dataStore;

  private Integer nodeId;

  private DynamicMarshal dynamicMarshal = null;

  private String outputHHPath;

  private Logger LOGGER = LoggerFactory.getLogger(UnsortedDataJobRunner.class);

  public UnsortedDataJobRunner(DataDescription dataDescription, DataStore dataStore, String nodeId,
      String outputHHPath) {
    this.dataStore = dataStore;
    this.nodeId = Integer.parseInt(nodeId);
    dynamicMarshal = new DynamicMarshal(dataDescription);
    this.outputHHPath = outputHHPath;
  }

  public void run(int primaryDimensionIndex, JobEntity jobEntity) {
    try {
      LOGGER.info("Starting execution of job: {}", jobEntity.getJobId());
      MemoryStatus.verifyMinimumMemoryRequirementIsFulfiled(
          DataRowProcessor.MINIMUM_FREE_MEMORY_REQUIRED_TO_BE_AVAILABLE_IN_MBS);
      StoreAccess storeAccess = dataStore.getStoreAccess(primaryDimensionIndex);
      RowProcessor rowProcessor =
          new DataRowProcessor(dynamicMarshal, jobEntity, outputHHPath, storeAccess);
      rowProcessor.process();
      HungryHipposFileSystem.getInstance().updateFSBlockMetaData(outputHHPath, nodeId,
          (new File(FileSystemContext.getRootDirectory() + outputHHPath)).length());
      LOGGER.info("Finished with execution of job: {}", jobEntity.getJobId());
    } catch (Exception e) {
      LOGGER.error(e.toString());
      throw new RuntimeException(e);
    }
  }

  @Override
  public void run(String jobUuid, List<PrimaryDimensionwiseJobsCollection> jobsCollectionList) {
    for (PrimaryDimensionwiseJobsCollection jobSCollection : jobsCollectionList) {
      int primaryDimensionIndex = jobSCollection.getPrimaryDimensionIndex();
      for (int i = 0; i < jobSCollection.getNumberOfJobs(); i++) {
        JobEntity jobEntity = jobSCollection.jobAt(i);
        int jobEntityId = jobEntity.getJobId();
        JobStatusNodeCoordinator.updateStartedJobEntity(jobUuid, jobEntityId, nodeId);
        run(primaryDimensionIndex, jobEntity);
        JobStatusNodeCoordinator.updateCompletedJobEntity(jobUuid, jobEntityId, nodeId);
      }
    }
  }

}
