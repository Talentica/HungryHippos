package com.talentica.hungryHippos.node.job;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.common.DataRowProcessor;
import com.talentica.hungryHippos.common.job.PrimaryDimensionwiseJobsCollection;
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.RowProcessor;
import com.talentica.hungryHippos.storage.StoreAccess;
import com.talentica.hungryHippos.storage.sorting.DataFileSorter;
import com.talentica.hungryHippos.storage.sorting.InsufficientMemoryException;
import com.talentica.hungryHippos.utility.JobEntity;
import com.talentica.hungryHippos.utility.MemoryStatus;
import com.talentica.hungryhippos.filesystem.HungryHipposFileSystem;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;

public class SortedDataJobRunner implements JobRunner {

  private static final long serialVersionUID = -4997999584207490930L;
  private Logger LOGGER = LoggerFactory.getLogger(SortedDataJobRunner.class);
  private DataStore dataStore;
  private int nodeId;
  private DynamicMarshal dynamicMarshal = null;
  private String outputHHPath;
  private DataFileSorter dataFileSorter;

  public SortedDataJobRunner(DataDescription dataDescription, DataStore dataStore, String nodeId,
      String outputHHPath, ShardingApplicationContext context)
      throws IOException, InsufficientMemoryException {
    this.dataStore = dataStore;
    this.nodeId = Integer.parseInt(nodeId);
    dynamicMarshal = new DynamicMarshal(dataDescription);
    this.outputHHPath = outputHHPath;
    this.dataFileSorter = new DataFileSorter(outputHHPath, context);
    this.dataFileSorter.doSortingDefault();
  }

  public void run(int primaryDimensionIndex, JobEntity jobEntity) {
    try {
      MemoryStatus.verifyMinimumMemoryRequirementIsFulfiled(
          DataRowProcessor.MINIMUM_FREE_MEMORY_REQUIRED_TO_BE_AVAILABLE_IN_MBS);
      dataFileSorter.doSortingJobWise(primaryDimensionIndex, jobEntity.getJob());
      StoreAccess storeAccess = dataStore.getStoreAccess(primaryDimensionIndex);
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

  @Override
  public void run(String jobUuid, List<PrimaryDimensionwiseJobsCollection> jobsCollectionList) {

  }

}
