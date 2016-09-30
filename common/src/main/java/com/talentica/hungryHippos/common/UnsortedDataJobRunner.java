package com.talentica.hungryHippos.common;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.DataDescription;
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

  private String nodeId;

  private DynamicMarshal dynamicMarshal = null;

  private String outputHHPath;

  private Logger LOGGER = LoggerFactory.getLogger(UnsortedDataJobRunner.class);

  public UnsortedDataJobRunner(DataDescription dataDescription, DataStore dataStore, String nodeId,
      String outputHHPath) {
    this.dataStore = dataStore;
    this.nodeId = nodeId;
    dynamicMarshal = new DynamicMarshal(dataDescription);
    this.outputHHPath = outputHHPath;
  }

  @Override
  public void run(JobEntity jobEntity) {
    try {
      MemoryStatus.verifyMinimumMemoryRequirementIsFulfiled(
          DataRowProcessor.MINIMUM_FREE_MEMORY_REQUIRED_TO_BE_AVAILABLE_IN_MBS);
      StoreAccess storeAccess = dataStore.getStoreAccess(jobEntity.getJob().getPrimaryDimension());
      RowProcessor rowProcessor =
          new DataRowProcessor(dynamicMarshal, jobEntity, outputHHPath, storeAccess);
      rowProcessor.process();
      HungryHipposFileSystem.getInstance().updateFSBlockMetaData(outputHHPath, nodeId,
          (new File(FileSystemContext.getRootDirectory() + outputHHPath)).length());
    } catch (Exception e) {
      LOGGER.error(e.toString());
      throw new RuntimeException(e);
    }
  }

}
