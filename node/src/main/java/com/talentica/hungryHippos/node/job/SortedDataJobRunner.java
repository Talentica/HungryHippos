package com.talentica.hungryHippos.node.job;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.common.SortedDataRowProcessor;
import com.talentica.hungryHippos.common.UnsortedDataRowProcessor;
import com.talentica.hungryHippos.common.job.PrimaryDimensionwiseJobsCollection;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
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

/**
 * {@code SortedDataJobRunner} used for running jobs on sorted data.
 *
 */
public class SortedDataJobRunner implements JobRunner {

  private static final long serialVersionUID = -4997999584207490930L;
  private Logger LOGGER = LoggerFactory.getLogger(SortedDataJobRunner.class);
  private static final String LOCK_FILE = "lock";
  private static final String SORTED_FILE = "sorted";
  private DataStore dataStore;
  private int nodeId;
  private DynamicMarshal dynamicMarshal;
  private String inputHHPath;
  private String outputHHPath;
  private DataFileSorter dataFileSorter;
  private DataDescription dataDescription;
  private ShardingApplicationContext context;

  /**
   * creates an instance of SortedDataJobRunner.
   * 
   * @param dataDescription
   * @param dataStore
   * @param nodeId
   * @param inputHHPath
   * @param outputHHPath
   * @param context
   * @throws IOException
   */
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
    this.validateAndStartDefaultSorting(inputHHPath, context);
  }

  public void run(int primaryDimensionIndex, List<JobEntity> jobEntities) {
    try {
      MemoryStatus.verifyMinimumMemoryRequirementIsFulfiled(
          UnsortedDataRowProcessor.MINIMUM_FREE_MEMORY_REQUIRED_TO_BE_AVAILABLE_IN_MBS);
      StoreAccess storeAccess = dataStore.getStoreAccess(primaryDimensionIndex);
      RowProcessor rowProcessor = new SortedDataRowProcessor(dynamicMarshal, jobEntities,
          outputHHPath, storeAccess, primaryDimensionIndex, dataDescription, context);     
      rowProcessor.process();
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
        LOGGER.info("Sorting started for primary dimension {}", primaryDimensionIndex);
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
      HungryHipposFileSystem.getInstance().updateFSBlockMetaData(outputHHPath, nodeId,
          (new File(FileSystemContext.getRootDirectory() + outputHHPath)).length());

    } catch (IOException | ClassNotFoundException | KeeperException | InterruptedException
        | JAXBException | HungryHippoException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void startSorting(String destinationPath, ShardingApplicationContext context) {
    long startTime = System.currentTimeMillis();
    try {
      dataFileSorter.doSortingDefault();
      long endTime = System.currentTimeMillis();
      LOGGER.info("Default sorting time in ms {}", (endTime - startTime));
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private synchronized void validateAndStartDefaultSorting(String inputHHPath,
      ShardingApplicationContext context) throws IOException {
    File sortedFile = new File(inputHHPath + File.separatorChar + SORTED_FILE);
    if (!checkSortedFileExists(sortedFile)) {
      this.startSorting(inputHHPath, context);
      createSortedFile(sortedFile);
    } else {
      LOGGER.info("Default files are already sorted");
    }
  }

  private synchronized boolean checkSortedFileExists(File sortedFile) throws IOException {
    LOGGER.info("Sorted file path {}", sortedFile.getAbsolutePath());
    if (sortedFile.exists()) {
      return true;
    }
    return false;
  }

  private synchronized void createSortedFile(File sortedFile) throws IOException {
    if (!sortedFile.exists()) {
      sortedFile.createNewFile();
    }
  }

}
