package com.talentica.hungryHippos.node.job;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.common.UnsortedDataRowProcessor;
import com.talentica.hungryHippos.common.job.PrimaryDimensionwiseJobsCollection;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
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
      MemoryStatus.verifyMinimumMemoryRequirementIsFulfiled(
          UnsortedDataRowProcessor.MINIMUM_FREE_MEMORY_REQUIRED_TO_BE_AVAILABLE_IN_MBS);
      StoreAccess storeAccess = dataStore.getStoreAccess(primaryDimensionIndex);
      RowProcessor rowProcessor =
          new UnsortedDataRowProcessor(dynamicMarshal, jobEntity, outputHHPath, storeAccess);
      rowProcessor.process();
    } catch (Exception e) {
      LOGGER.error(e.toString());
      throw new RuntimeException(e);
    }
  }

  @Override
  public void run(String jobUuid, List<PrimaryDimensionwiseJobsCollection> jobsCollectionList) {
    try {
      for (PrimaryDimensionwiseJobsCollection dimensionWiseJob : jobsCollectionList) {
        int primaryDimensionIndex = dimensionWiseJob.getPrimaryDimensionIndex();
        LOGGER.info("Executing {} jobs for primary dimension: {}",
            new Object[] {dimensionWiseJob.getNumberOfJobs(), primaryDimensionIndex});
        for (int i = 0; i < dimensionWiseJob.getNumberOfJobs(); i++) {
          JobEntity jobEntity = dimensionWiseJob.jobAt(i);
          int jobEntityId = jobEntity.getJobId();
          JobStatusNodeCoordinator.updateStartedJobEntity(jobUuid, jobEntityId, nodeId);
          LOGGER.info("Execution of job: {} started", new Object[] {jobEntity});
          run(primaryDimensionIndex, jobEntity);
          JobStatusNodeCoordinator.updateCompletedJobEntity(jobUuid, jobEntityId, nodeId);
          LOGGER.info("Execution of job: {} finished", new Object[] {jobEntity});
        }
      }
      HungryHipposFileSystem.getInstance().updateFSBlockMetaData(outputHHPath, nodeId,
          (new File(FileSystemContext.getRootDirectory() + outputHHPath)).length());
    } catch (FileNotFoundException | HungryHippoException | JAXBException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

}
