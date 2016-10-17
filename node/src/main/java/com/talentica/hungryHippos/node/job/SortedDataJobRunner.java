package com.talentica.hungryHippos.node.job;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.common.DataRowProcessor;
import com.talentica.hungryHippos.common.SortedDataRowProcessor;
import com.talentica.hungryHippos.common.job.PrimaryDimensionwiseJobsCollection;
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.RowProcessor;
import com.talentica.hungryHippos.storage.StoreAccess;
import com.talentica.hungryHippos.storage.sorting.DataFileSorter;
import com.talentica.hungryHippos.utility.JobEntity;
import com.talentica.hungryHippos.utility.MemoryStatus;
import com.talentica.hungryhippos.filesystem.HungryHipposFileSystem;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;

public class SortedDataJobRunner implements JobRunner {

  private static final long serialVersionUID = -4997999584207490930L;
  private Logger LOGGER = LoggerFactory.getLogger(SortedDataJobRunner.class);
  private static final String LOCK_FILE = "lock";
  private DataStore dataStore;
  private int nodeId;
  private DynamicMarshal dynamicMarshal;
  private String inputHHPath;
  private String outputHHPath;
  private DataFileSorter dataFileSorter;
  private DataDescription dataDescription;
  private ShardingApplicationContext context;

  public SortedDataJobRunner(DataDescription dataDescription, DataStore dataStore, String nodeId,
      String inputHHPath, String outputHHPath, ShardingApplicationContext context)
      throws IOException {
    this.dataStore = dataStore;
    this.nodeId = Integer.parseInt(nodeId);
    this.dynamicMarshal = new DynamicMarshal(dataDescription);
    this.inputHHPath = inputHHPath;
    this.outputHHPath = outputHHPath;
    this.dataDescription = dataDescription;
    this.context = context;
    this.dataFileSorter = new DataFileSorter(this.inputHHPath, context);
  }

  public void run(int primaryDimensionIndex, List<JobEntity> jobEntities) {
    LOGGER.info("Start running the jobs {} having primary dimension {}",
        Arrays.toString(jobEntities.toArray()), primaryDimensionIndex);
    try {
      MemoryStatus.verifyMinimumMemoryRequirementIsFulfiled(
          DataRowProcessor.MINIMUM_FREE_MEMORY_REQUIRED_TO_BE_AVAILABLE_IN_MBS);
      StoreAccess storeAccess = dataStore.getStoreAccess(primaryDimensionIndex);
      RowProcessor rowProcessor = new SortedDataRowProcessor(dynamicMarshal, jobEntities,
          outputHHPath, storeAccess, primaryDimensionIndex, dataDescription, context);
      rowProcessor.process();
      HungryHipposFileSystem.getInstance().updateFSBlockMetaData(outputHHPath, nodeId,
          (new File(FileSystemContext.getRootDirectory() + outputHHPath)).length());
    } catch (Exception e) {
      LOGGER.error(e.toString());
      throw new RuntimeException(e);
    }
  }

  private synchronized void waitFileUnlock() {
    File lockFile = new File(this.inputHHPath + File.separatorChar + LOCK_FILE);
    while (true) {
      if (!lockFile.exists()) {
        break;
      }
    }
  }

  @Override
  public void run(String jobUuid, List<PrimaryDimensionwiseJobsCollection> jobsCollectionList) {
    waitFileUnlock();
    try {
      for (PrimaryDimensionwiseJobsCollection jobSCollection : jobsCollectionList) {
        int primaryDimensionIndex = jobSCollection.getPrimaryDimensionIndex();
        dataFileSorter.doSortingPrimaryDimensionWise(primaryDimensionIndex);
        for (int i = 0; i < jobSCollection.getNumberOfJobs(); i++) {
          JobEntity jobEntity = jobSCollection.jobAt(i);
          int jobEntityId = jobEntity.getJobId();
          JobStatusNodeCoordinator.updateStartedJobEntity(jobUuid, jobEntityId, nodeId);
        }

        run(primaryDimensionIndex, jobSCollection.getJobs());

        for (int i = 0; i < jobSCollection.getNumberOfJobs(); i++) {
          JobEntity jobEntity = jobSCollection.jobAt(i);
          int jobEntityId = jobEntity.getJobId();
          JobStatusNodeCoordinator.updateCompletedJobEntity(jobUuid, jobEntityId, nodeId);
        }
      }
    } catch (IOException | ClassNotFoundException | KeeperException | InterruptedException
        | JAXBException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
