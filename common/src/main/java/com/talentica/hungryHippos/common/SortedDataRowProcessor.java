/**
 * 
 */
package com.talentica.hungryHippos.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.ValueSet;
import com.talentica.hungryHippos.client.domain.Work;
import com.talentica.hungryHippos.common.context.JobRunnerApplicationContext;
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.storage.DataFileAccess;
import com.talentica.hungryHippos.storage.RowProcessor;
import com.talentica.hungryHippos.storage.StoreAccess;
import com.talentica.hungryHippos.storage.sorting.BinaryFileBuffer;
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
  private Map<ValueSet, TreeMap<ValueSet, Work>> jobToValuesetWorkMap =
      new TreeMap<ValueSet, TreeMap<ValueSet, Work>>();
  private Logger LOGGER = LoggerFactory.getLogger(DataRowProcessor.class);
  private int totalNoOfRowsProcessed = 0;
  private StoreAccess storeAccess;
  private int primaryDimension;
  private DataDescription dataDescription;
  private ShardingApplicationContext context;
  private int[] shardDims;
  public static final long MINIMUM_FREE_MEMORY_REQUIRED_TO_BE_AVAILABLE_IN_MBS =
      JobRunnerApplicationContext.getZkJobRunnerConfig().getMinFreeMemoryInMbs();

  long startTime = System.currentTimeMillis();

  public SortedDataRowProcessor(DynamicMarshal dynamicMarshal, List<JobEntity> jobEntities,
      String outputHHPath, StoreAccess storeAccess, int primaryDimension,
      DataDescription dataDescription, ShardingApplicationContext context) throws IOException {
    this.jobEntities = jobEntities;
    this.dynamicMarshal = dynamicMarshal;
    this.storeAccess = storeAccess;
    this.primaryDimension = primaryDimension;
    this.executionContext = new ExecutionContextImpl(dynamicMarshal, outputHHPath);
    this.dataDescription = dataDescription;
    this.context = context;
    this.shardDims = context.getShardingIndexes();
    this.buildDataFileAccess();
    this.shardDims = orderDimensions(primaryDimension);
  }


  @Override
  public void process() {
    ByteBuffer row;
    try {
      while (pq.size() > 0) {
        BinaryFileBuffer bfb = pq.poll();
        row = bfb.pop();
        processRow(row);
        if (bfb.empty()) {
          bfb.getReader().close();
        } else {
          pq.add(bfb);
        }
      }
    } catch (IOException e) {
      LOGGER.error(e.toString());
      throw new RuntimeException(e);
    }
  }

  private void processRow(ByteBuffer row) {
    for (JobEntity jobEntity : jobEntities) {
      ValueSet valueSet = new ValueSet(jobEntity.getJob().getDimensions());
      ValueSet flagValueSet = new ValueSet(jobEntity.getDimensionsFlush());
      for (int i = 0; i < jobEntity.getJob().getDimensions().length; i++) {
        Object value = dynamicMarshal.readValue(jobEntity.getJob().getDimensions()[i], row);
        valueSet.setValue(value, i);
      }

      for (int i = 0; i < jobEntity.getDimensionsFlush().length; i++) {
        Object value = dynamicMarshal.readValue(jobEntity.getDimensionsFlush()[i], row);
        flagValueSet.setValue(value, i);
      }

      Work reducer = prepareReducersBatch(valueSet, flagValueSet, jobEntity);
      processReducers(reducer, row);
    }
  }

  private Work prepareReducersBatch(ValueSet valueSet, ValueSet flagValueSet, JobEntity jobEntity) {
    Work reducer = null;
    reducer = addReducer(valueSet, flagValueSet, jobEntity);
    logProgress();
    return reducer;
  }

  private void logProgress() {
    totalNoOfRowsProcessed++;
    if (totalNoOfRowsProcessed % MAXIMUM_NO_OF_ROWS_TO_LOG_PROGRESS_AFTER == 0) {
      LOGGER.info(
          "*********  Processing in progress. {} no. of rows processed... and current batch size {} *********",
          new Object[] {totalNoOfRowsProcessed, jobToValuesetWorkMap.size()});
      LOGGER.info(
          "Memory status (in MBs): Max free memory available-{}, Used memory-{} ,Total Memory-{}, Free memory {},Max memory-{}.",
          new Object[] {MemoryStatus.getMaximumFreeMemoryThatCanBeAllocated(),
              MemoryStatus.getUsedMemory(), MemoryStatus.getTotalmemory(),
              MemoryStatus.getFreeMemory(), MemoryStatus.getMaxMemory()});
      LOGGER.info("Size of batch(jobToValuesetWorkMap) is: {}",
          new Object[] {jobToValuesetWorkMap.size()});
    }
  }

  private void processReducers(Work work, ByteBuffer row) {
    if (work != null) {
      executionContext.setData(row);
      work.processRow(executionContext);
    }
  }

  private Work addReducer(ValueSet valueSet, ValueSet flagValueSet, JobEntity jobEntity) {
    TreeMap<ValueSet, Work> valuesetToWorkTreeMap = jobToValuesetWorkMap.get(flagValueSet);
    if (valuesetToWorkTreeMap == null) {
      TreeMap<ValueSet, Work> actualValuesetToWorkTreeMap = jobToValuesetWorkMap.get(valueSet);
      finishUp(actualValuesetToWorkTreeMap);
      actualValuesetToWorkTreeMap.clear();
      jobToValuesetWorkMap.put(flagValueSet, actualValuesetToWorkTreeMap);
    }
    TreeMap<ValueSet, Work> newValueSetToTreeMap = jobToValuesetWorkMap.get(flagValueSet);
    Work work = newValueSetToTreeMap.get(valueSet);
    if (work == null) {
      work = jobEntity.getJob().createNewWork();
      newValueSetToTreeMap.put(valueSet, work); // put new value set
    }
    return work;
  }


  private void finishUp(TreeMap<ValueSet, Work> valuesetToWorkTreeMap) {
    for (Entry<ValueSet, Work> e : valuesetToWorkTreeMap.entrySet()) {
      executionContext.setKeys(e.getKey());
      e.getValue().calculate(executionContext);
    }
  }

  private void buildDataFileAccess() throws IOException {
    for (DataFileAccess dataFolder : storeAccess) {
      for (DataFileAccess dataFile : dataFolder) {
        BinaryFileBuffer bfb =
            new BinaryFileBuffer(dataFile.getDataFileInputStream(), dataDescription.getSize());
        if (!bfb.empty()) {
          pq.add(bfb);
        }
      }
    }
  }

  private void prepareJobsFlushFlag() {
    for (JobEntity jobEntity : jobEntities) {
      List<Integer> dimns = new ArrayList<>();
      for (int jobDim = 0; jobDim < jobEntity.getJob().getDimensions().length; jobDim++) {
        for (int index = 0; index < context.getShardingIndexes().length; index++) {
          if (jobEntity.getJob().getDimensions()[jobDim] == context.getShardingIndexes()[index]) {
            dimns.add(jobEntity.getJob().getDimensions()[jobDim]);
          }
        }
      }
      jobEntity.setDimensionsFlush(dimns.stream().mapToInt(i -> i).toArray());
    }
  }

  private int compareRow(byte[] row1, byte[] row2) {
    int res = 0;
    DataLocator locator = dataDescription.locateField(primaryDimension);
    int columnPos = locator.getOffset();
    for (int pointer = 0; pointer < locator.getSize(); pointer++) {
      if (row1[columnPos] != row2[columnPos]) {
        return row1[columnPos] - row2[columnPos];
      }
      columnPos++;
    }
    return res;
  }


  private PriorityQueue<BinaryFileBuffer> pq =
      new PriorityQueue<>(11, new Comparator<BinaryFileBuffer>() {
        @Override
        public int compare(BinaryFileBuffer i, BinaryFileBuffer j) {
          return compareRow(i.peek().array(), j.peek().array());
        }
      });


  private int[] orderDimensions(int primaryDimension) {
    int[] sortDims = new int[shardDims.length];
    for (int i = 0; i < shardDims.length; i++) {
      sortDims[i] = shardDims[(i + primaryDimension) % shardDims.length];
    }
    return sortDims;
  }


}
