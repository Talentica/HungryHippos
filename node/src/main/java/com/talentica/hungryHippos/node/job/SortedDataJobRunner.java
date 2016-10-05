package com.talentica.hungryHippos.node.job;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
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
import com.talentica.hungryHippos.utility.JobEntity;
import com.talentica.hungryHippos.utility.MemoryStatus;
import com.talentica.hungryhippos.filesystem.HungryHipposFileSystem;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;

public class SortedDataJobRunner implements JobRunner {

  private static final long serialVersionUID = -4997999584207490930L;
  private Logger LOGGER = LoggerFactory.getLogger(SortedDataJobRunner.class);
  private static final String LOCK_FILE = "lock";
  private static final String primDimSort = "prim-dim";
  private DataStore dataStore;
  private int nodeId;
  private DynamicMarshal dynamicMarshal = null;
  private String inputHHPath;
  private DataFileSorter dataFileSorter;
  private File primDimFile;

  public SortedDataJobRunner(DataDescription dataDescription, DataStore dataStore, String nodeId,
      String inputHHPath, ShardingApplicationContext context) throws IOException {
    this.dataStore = dataStore;
    this.nodeId = Integer.parseInt(nodeId);
    dynamicMarshal = new DynamicMarshal(dataDescription);
    this.inputHHPath = inputHHPath;
    this.dataFileSorter = new DataFileSorter(this.inputHHPath, context);
    this.primDimFile = new File(this.inputHHPath + File.separatorChar + primDimSort);
  }

  public void run(int primaryDimensionIndex, JobEntity jobEntity) {
    try {
      MemoryStatus.verifyMinimumMemoryRequirementIsFulfiled(
          DataRowProcessor.MINIMUM_FREE_MEMORY_REQUIRED_TO_BE_AVAILABLE_IN_MBS);
      StoreAccess storeAccess = dataStore.getStoreAccess(primaryDimensionIndex);
      RowProcessor rowProcessor =
          new DataRowProcessor(dynamicMarshal, jobEntity, inputHHPath, storeAccess);
      rowProcessor.process();
      HungryHipposFileSystem.getInstance().updateFSBlockMetaData(inputHHPath, nodeId,
          (new File(FileSystemContext.getRootDirectory() + inputHHPath)).length());
    } catch (Exception e) {
      LOGGER.error(e.toString());
      throw new RuntimeException(e);
    }
  }

  private void waitFileUnlock() {
    File lockFile = new File(this.inputHHPath + File.separatorChar + LOCK_FILE);
    while (true) {
      if (!lockFile.exists()) {
        break;
      }
    }
  }

  private void setPrimDimForSorting(int primDim) throws IOException {
    FileOutputStream fos = new FileOutputStream(primDimFile, false);
    fos.write(primDim);
    fos.flush();
    fos.close();
  }

  @SuppressWarnings("resource")
  private boolean isFileSortedOnPrimDim(int primDim) throws IOException {
    if (!primDimFile.exists()) {
      primDimFile.createNewFile();
    }
    FileInputStream fis = new FileInputStream(primDimFile);
    if (primDimFile.exists()) {
      if ((int) fis.read() == primDim) {
        return true;
      }
    }
    fis.close();
    return false;
  }

  @Override
  public void run(String jobUuid, List<PrimaryDimensionwiseJobsCollection> jobsCollectionList) {
    waitFileUnlock();
    try {
      for (PrimaryDimensionwiseJobsCollection jobSCollection : jobsCollectionList) {
        int primaryDimensionIndex = jobSCollection.getPrimaryDimensionIndex();
        boolean isPrimDimSorted = isFileSortedOnPrimDim(primaryDimensionIndex);
        if (!isPrimDimSorted) {
          dataFileSorter.doSortingJobWise(primaryDimensionIndex);
          setPrimDimForSorting(primaryDimensionIndex);
        }
        for (int i = 0; i < jobSCollection.getNumberOfJobs(); i++) {
          JobEntity jobEntity = jobSCollection.jobAt(i);
          int jobEntityId = jobEntity.getJobId();
          JobStatusNodeCoordinator.updateStartedJobEntity(jobUuid, jobEntityId, nodeId);
          run(primaryDimensionIndex, jobEntity);
          JobStatusNodeCoordinator.updateCompletedJobEntity(jobUuid, jobEntityId, nodeId);
        }
      }
    } catch (IOException | ClassNotFoundException | KeeperException | InterruptedException
        | JAXBException e) {
      LOGGER.error(e.toString());
    }
  }
}
