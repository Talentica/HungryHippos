/**
 * 
 */
package com.talentica.hungryHippos.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
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
  private Map<Integer, TreeMap<ValueSet, Work>> jobToValuesetWorkMap =
      new TreeMap<Integer, TreeMap<ValueSet, Work>>();
  private Logger LOGGER = LoggerFactory.getLogger(DataRowProcessor.class);
  private int totalNoOfRowsProcessed = 0;
  private StoreAccess storeAccess;
  private DataDescription dataDescription;
  private ShardingApplicationContext context;
  private int[] sharDim;
  private Map<Integer, ValueSet> currentValueSetPointerJobMap;
  private Map<Integer, ValueSet> lastValueSetPointerJobMap;
  public static final long MINIMUM_FREE_MEMORY_REQUIRED_TO_BE_AVAILABLE_IN_MBS =
      JobRunnerApplicationContext.getZkJobRunnerConfig().getMinFreeMemoryInMbs();

  long startTime = System.currentTimeMillis();

  public SortedDataRowProcessor(DynamicMarshal dynamicMarshal, List<JobEntity> jobEntities,
      String outputHHPath, StoreAccess storeAccess, int primaryDimension,
      DataDescription dataDescription, ShardingApplicationContext context) throws IOException {
    this.jobEntities = jobEntities;
    this.dynamicMarshal = dynamicMarshal;
    this.storeAccess = storeAccess;
    this.executionContext = new ExecutionContextImpl(dynamicMarshal, outputHHPath);
    this.dataDescription = dataDescription;
    this.context = context;
    this.sharDim = new int[context.getShardingIndexes().length];
    this.orderDimensions(primaryDimension);
    this.buildDataFileAccess();
    this.currentValueSetPointerJobMap = new HashMap<Integer, ValueSet>();
    this.lastValueSetPointerJobMap = new HashMap<Integer, ValueSet>();
    this.prepareJobsFlushFlag();
  }


  @Override
  public void process() {
    try {
      while (pq.size() > 0) {
        BinaryFileBuffer bfb = pq.poll();
        run(bfb);
      }
      flushRemaining();
    } catch (IOException e) {
      LOGGER.error(e.toString());
      throw new RuntimeException(e);
    }
  }


  private void run(BinaryFileBuffer bfb) throws IOException {
    ByteBuffer row;
    row = bfb.pop();
    processRow(row);
    if (bfb.empty()) {
      bfb.getReader().close();
    } else {
      pq.add(bfb);
    }
  }


  private void flushRemaining() {
    for (JobEntity jobEntity : jobEntities) {
      TreeMap<ValueSet, Work> valuesetToWorkTreeMap =
          jobToValuesetWorkMap.get(jobEntity.getJobId());
      LOGGER.info("Flusing for job id {}", jobEntity.getJobId());
      if (!valuesetToWorkTreeMap.isEmpty()) {
        finishUp(valuesetToWorkTreeMap);
        valuesetToWorkTreeMap.clear();
      }
    }
  }

  private void processRow(ByteBuffer row) {
    for (JobEntity jobEntity : jobEntities) {
      ValueSet valueSet = new ValueSet(jobEntity.getJob().getDimensions());
      ValueSet valueSetPointer = getValueSetPointer(row, jobEntity);
      boolean isJobFlushable = prepareValueSet(row, jobEntity, valueSet, valueSetPointer);
      Work reducer = prepareReducersBatch(valueSet, isJobFlushable, jobEntity);
      processReducers(reducer, row);
    }
  }


  private boolean prepareValueSet(ByteBuffer row, JobEntity jobEntity, ValueSet valueSet,
      ValueSet currentValueSetPointer) {
    boolean isFlushable = false;
    for (int i = 0; i < jobEntity.getJob().getDimensions().length; i++) {
      Object value = dynamicMarshal.readValue(jobEntity.getJob().getDimensions()[i], row);
      valueSet.setValue(value, i);
    }
    if (lastValueSetPointerJobMap.get(jobEntity.getJobId()) == null) {
      ValueSet vsIdentifier = new ValueSet(jobEntity.getDimensionsPointer());
      for (int i = 0; i < jobEntity.getDimensionsPointer().length; i++) {
        Object value = dynamicMarshal.readValue(jobEntity.getDimensionsPointer()[i], row);
        vsIdentifier.setValue(value, i);
      }
      lastValueSetPointerJobMap.put(jobEntity.getJobId(), vsIdentifier);
    } else {
      ValueSet lastValueSetPointer = lastValueSetPointerJobMap.get(jobEntity.getJobId());
      if (lastValueSetPointer.compareTo(currentValueSetPointer) != 0) {
        isFlushable = true;
        lastValueSetPointerJobMap.put(jobEntity.getJobId(),
            currentValueSetPointer.copy(lastValueSetPointer));
      }
    }
    return isFlushable;
  }


  private ValueSet getValueSetPointer(ByteBuffer row, JobEntity jobEntity) {
    ValueSet valueSetPointer = currentValueSetPointerJobMap.get(jobEntity.getJobId());
    if (valueSetPointer == null) {
      valueSetPointer = new ValueSet(jobEntity.getDimensionsPointer());
    }
    for (int i = 0; i < jobEntity.getDimensionsPointer().length; i++) {
      Object value = dynamicMarshal.readValue(jobEntity.getDimensionsPointer()[i], row);
      valueSetPointer.setValue(value, i);
    }
    currentValueSetPointerJobMap.put(jobEntity.getJobId(), valueSetPointer);
    return valueSetPointer;
  }

  private Work prepareReducersBatch(ValueSet valueSet, boolean isJobFlushable,
      JobEntity jobEntity) {
    Work reducer = null;
    reducer = addReducer(valueSet, isJobFlushable, jobEntity);
    logProgress();
    return reducer;
  }

  private void logProgress() {
    totalNoOfRowsProcessed++;
    if (totalNoOfRowsProcessed % MAXIMUM_NO_OF_ROWS_TO_LOG_PROGRESS_AFTER == 0) {
      /*
       * LOGGER.info(
       * "*********  Processing in progress. {} no. of rows processed... and current batch size {} *********"
       * , new Object[] {totalNoOfRowsProcessed, jobToValuesetWorkMap.size()}); LOGGER.info(
       * "Memory status (in MBs): Max free memory available-{}, Used memory-{} ,Total Memory-{}, Free memory {},Max memory-{}."
       * , new Object[] {MemoryStatus.getMaximumFreeMemoryThatCanBeAllocated(),
       * MemoryStatus.getUsedMemory(), MemoryStatus.getTotalmemory(), MemoryStatus.getFreeMemory(),
       * MemoryStatus.getMaxMemory()}); LOGGER.info("Size of batch(jobToValuesetWorkMap) is: {}",
       * new Object[] {jobToValuesetWorkMap.size()});
       */
      LOGGER.info(
          "Memory status (in MBs): Max free memory available-{}, Used memory-{} ,Total Memory-{}, Free memory {},Max memory-{}.",
          new Object[] {MemoryStatus.getMaximumFreeMemoryThatCanBeAllocated(),
              MemoryStatus.getUsedMemory(), MemoryStatus.getTotalmemory(),
              MemoryStatus.getFreeMemory(), MemoryStatus.getMaxMemory()});
      System.gc();
    }
  }

  private void processReducers(Work work, ByteBuffer row) {
    if (work != null) {
      executionContext.setData(row);
      work.processRow(executionContext);
    }
  }

  private Work addReducer(ValueSet valueSet, boolean isJobFlush, JobEntity jobEntity) {
    TreeMap<ValueSet, Work> valuesetToWorkTreeMap = jobToValuesetWorkMap.get(jobEntity.getJobId());
    if (valuesetToWorkTreeMap == null) {
      valuesetToWorkTreeMap = new TreeMap<ValueSet, Work>();
      jobToValuesetWorkMap.put(jobEntity.getJobId(), valuesetToWorkTreeMap);
    }
    if (isJobFlush && !valuesetToWorkTreeMap.isEmpty()) {
      LOGGER.info("Flusing result for job id {}", jobEntity.getJobId());
      finishUp(valuesetToWorkTreeMap);
      valuesetToWorkTreeMap.clear();
    }
    Work work = valuesetToWorkTreeMap.get(valueSet);
    if (work == null) {
      work = jobEntity.getJob().createNewWork();
      valuesetToWorkTreeMap.put(valueSet, work); // put new value set
      jobToValuesetWorkMap.put(jobEntity.getJobId(), valuesetToWorkTreeMap);
    }
    return work;
  }

  private void finishUp(TreeMap<ValueSet, Work> valuesetToWorkTreeMap) {
    for (Entry<ValueSet, Work> e : valuesetToWorkTreeMap.entrySet()) {
      executionContext.setKeys(e.getKey());
      e.getValue().calculate(executionContext);
    }
    executionContext.flush();
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
    List<Integer> dimns = new ArrayList<>();
    for (JobEntity jobEntity : jobEntities) {
      for (int jobDim = 0; jobDim < jobEntity.getJob().getDimensions().length; jobDim++) {
        for (int index = 0; index < context.getShardingIndexes().length; index++) {
          if (jobEntity.getJob().getDimensions()[jobDim] == context.getShardingIndexes()[index]) {
            dimns.add(jobEntity.getJob().getDimensions()[jobDim]);
          }
        }
      }
      jobEntity.setDimensionsPointer(dimns.stream().mapToInt(i -> i).toArray());
      dimns.clear();
    }
    LOGGER.info("All recent jobs {}", Arrays.toString(jobEntities.toArray()));
  }


  private int columnPos = 0;

  private int compareRow(byte[] row1, byte[] row2) {
    int res = 0;
    for (int dim = 0; dim < sharDim.length; dim++) {
      DataLocator locator = dataDescription.locateField(sharDim[dim]);
      columnPos = locator.getOffset();
      for (int pointer = 0; pointer < locator.getSize(); pointer++) {
        if (row1[columnPos] != row2[columnPos]) {
          return row1[columnPos] - row2[columnPos];
        }
        columnPos++;
      }
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

  private void orderDimensions(int primaryDimension) {
    for (int i = 0; i < context.getShardingIndexes().length; i++) {
      sharDim[i] = context.getShardingIndexes()[(i + primaryDimension)
          % context.getShardingIndexes().length];
    }
  }
}
