package com.talentica.hungryHippos.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;

import javax.xml.bind.JAXBException;

import com.talentica.hungryhippos.filesystem.HungryHipposFileSystem;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.StoreAccess;
import com.talentica.hungryHippos.utility.JobEntity;
import com.talentica.hungryHippos.utility.MemoryStatus;

/**
 * Created by debasishc on 9/9/15.
 */
public class JobRunner implements Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = -4793614653018059851L;
  private DataStore dataStore;
  private String nodeId;
  private DynamicMarshal dynamicMarshal = null;
  private String outputHHPath;

  private Logger LOGGER = LoggerFactory.getLogger(JobRunner.class);

  public JobRunner(DataDescription dataDescription, DataStore dataStore, String nodeId,String outputHHPath) {
    this.dataStore = dataStore;
    this.nodeId = nodeId;
    dynamicMarshal = new DynamicMarshal(dataDescription);
    this.outputHHPath = outputHHPath;
  }

  public void run(JobEntity jobEntity) throws ClassNotFoundException, FileNotFoundException,
      KeeperException, InterruptedException, IOException, JAXBException {
    checkIfMemoryAvailableToRunJob();
    StoreAccess storeAccess = null;
    storeAccess = dataStore.getStoreAccess(jobEntity.getJob().getPrimaryDimension());
    DataRowProcessor rowProcessor = new DataRowProcessor(dynamicMarshal, jobEntity, outputHHPath);
    storeAccess.addRowProcessor(rowProcessor);
    do {
      storeAccess.processRows();
      rowProcessor.finishUp();
    } while (rowProcessor.isAdditionalValueSetsPresentForProcessing());

    try {
      HungryHipposFileSystem.getInstance().updateFSBlockMetaData(outputHHPath, nodeId, (new File(FileSystemContext.getRootDirectory()+outputHHPath)).length());
    } catch (Exception e) {
      LOGGER.error(e.toString());
     throw new RuntimeException(e);
    }
  }

  private void checkIfMemoryAvailableToRunJob() {
    long freeMemory = MemoryStatus.getMaximumFreeMemoryThatCanBeAllocated();
    if (freeMemory <= DataRowProcessor.MINIMUM_FREE_MEMORY_REQUIRED_TO_BE_AVAILABLE_IN_MBS) {
      LOGGER.error(
          "Either very less memory:{} MBs is available to run jobs or the amount of threshold memory:{} MBs configured is too high.",
          new Object[] {freeMemory,
              DataRowProcessor.MINIMUM_FREE_MEMORY_REQUIRED_TO_BE_AVAILABLE_IN_MBS});
      throw new RuntimeException(
          "Either very less memory is available to run jobs or the amount of threshold memory configured is too high.");
    }
  }

}
