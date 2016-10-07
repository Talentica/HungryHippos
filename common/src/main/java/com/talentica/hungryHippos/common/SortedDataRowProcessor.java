/**
 * 
 */
package com.talentica.hungryHippos.common;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.ValueSet;
import com.talentica.hungryHippos.client.domain.Work;
import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.common.context.JobRunnerApplicationContext;
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.storage.DataFileAccess;
import com.talentica.hungryHippos.storage.RowProcessor;
import com.talentica.hungryHippos.storage.StoreAccess;
import com.talentica.hungryHippos.utility.JobEntity;
import com.talentica.hungryHippos.utility.MemoryStatus;


/**
 * @author pooshans
 *
 */
public class SortedDataRowProcessor implements RowProcessor {
  private static final long MAXIMUM_NO_OF_ROWS_TO_LOG_PROGRESS_AFTER =
      JobRunnerApplicationContext.getZkJobRunnerConfig().getNoOfRowsToLogProgressAfter();

  private DynamicMarshal dynamicMarshal;
  private ExecutionContextImpl executionContext;
  private List<JobEntity> jobEntities;
  private TreeMap<ValueSet, Work> valuesetToWorkTreeMap = new TreeMap<>();
  private boolean additionalValueSetsPresentForProcessing = false;
  private Logger LOGGER = LoggerFactory.getLogger(DataRowProcessor.class);
  private int totalNoOfRowsProcessed = 0;
  private StoreAccess storeAccess;
  public static final long MINIMUM_FREE_MEMORY_REQUIRED_TO_BE_AVAILABLE_IN_MBS =
      JobRunnerApplicationContext.getZkJobRunnerConfig().getMinFreeMemoryInMbs();

  long startTime = System.currentTimeMillis();

  public SortedDataRowProcessor(DynamicMarshal dynamicMarshal, List<JobEntity> jobEntities,
      String outputHHPath, StoreAccess storeAccess) {
    this.jobEntities = jobEntities;
    this.dynamicMarshal = dynamicMarshal;
    this.storeAccess = storeAccess;
    this.executionContext = new ExecutionContextImpl(dynamicMarshal, outputHHPath);
  }

  @Override
  public void process() {
    for (DataFileAccess dataFolder : storeAccess) {
      for (DataFileAccess dataFile : dataFolder) {
        while (dataFile.isNextReadAvailable()) {
          processRow(dataFile.readNext());
        }
      }
    }
    if (!isAdditionalValueSetsPresentForProcessing()) {
      return;
    }
    process();
  }

  private void processRow(ByteBuffer row) {
    for (JobEntity jobEntity : jobEntities) {
      ValueSet valueSet = new ValueSet(jobEntity.getJob().getDimensions());
      for (int i = 0; i < jobEntity.getJob().getDimensions().length; i++) {
        Object value = dynamicMarshal.readValue(jobEntity.getJob().getDimensions()[i], row);
        valueSet.setValue(value, i);
      }
      Work reducer = prepareReducersBatch(valueSet, jobEntity.getJob());
      processReducers(reducer, row);
    }
  }

  private Work prepareReducersBatch(ValueSet valueSet, Job job) {
    Work reducer = null;
    reducer = addReducer(valueSet, job);
    logProgress();
    return reducer;
  }

  private void logProgress() {
    totalNoOfRowsProcessed++;
    if (totalNoOfRowsProcessed % MAXIMUM_NO_OF_ROWS_TO_LOG_PROGRESS_AFTER == 0) {
      LOGGER.info(
          "*********  Processing in progress. {} no. of rows processed... and current batch size {} *********",
          new Object[] {totalNoOfRowsProcessed, valuesetToWorkTreeMap.size()});
      LOGGER.info(
          "Memory status (in MBs): Max free memory available-{}, Used memory-{} ,Total Memory-{}, Free memory {},Max memory-{}.",
          new Object[] {MemoryStatus.getMaximumFreeMemoryThatCanBeAllocated(),
              MemoryStatus.getUsedMemory(), MemoryStatus.getTotalmemory(),
              MemoryStatus.getFreeMemory(), MemoryStatus.getMaxMemory()});
      LOGGER.info("Size of batch(valuesetToWorkTreeMap) is: {}",
          new Object[] {valuesetToWorkTreeMap.size()});
    }
  }

  private void processReducers(Work work, ByteBuffer row) {
    if (work != null) {
      executionContext.setData(row);
      work.processRow(executionContext);
    }
  }

  private Work addReducer(ValueSet valueSet, Job job) {
    Work work = valuesetToWorkTreeMap.get(valueSet);
    if (work == null) {
      for (ValueSet vs : valuesetToWorkTreeMap.keySet()) {
        if (Arrays.equals(valueSet.getKeyIndexes(), vs.getKeyIndexes())) {
          finishUp(vs);
          valuesetToWorkTreeMap.remove(vs);
          break;
        }
      }
      work = job.createNewWork();
      valuesetToWorkTreeMap.put(valueSet, work);
    }
    return work;
  }

  private void finishUp(ValueSet valueSet) {
    Work work = valuesetToWorkTreeMap.get(valueSet);
    executionContext.setKeys(valueSet);
    work.calculate(executionContext);
  }

  private boolean isAdditionalValueSetsPresentForProcessing() {
    return additionalValueSetsPresentForProcessing;
  }

}
