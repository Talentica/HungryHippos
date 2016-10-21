/**
 * 
 */
package com.talentica.hungryHippos.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
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
 * To perform the data row processing on sorted input data.
 * 
 * @author pooshans
 *
 */
public class SortedDataRowProcessor implements RowProcessor {
  private static final long MAXIMUM_NO_OF_ROWS_TO_LOG_PROGRESS_AFTER =
      JobRunnerApplicationContext.getZkJobRunnerConfig().getNoOfRowsToLogProgressAfter();

  private DynamicMarshal dynamicMarshal;
  private ExecutionContextImpl executionContext;
  private List<JobEntity> jobEntities;
  private Map<JobEntity, TreeMap<ValueSet, Work>> jobToValuesetWorkMap =
      new TreeMap<JobEntity, TreeMap<ValueSet, Work>>();
  private Logger LOGGER = LoggerFactory.getLogger(SortedDataRowProcessor.class);
  private int totalNoOfRowsProcessed = 0;
  private StoreAccess storeAccess;
  private DataDescription dataDescription;
  private ShardingApplicationContext context;
  private int[] sortedDimensionsOrder;
  private Map<JobEntity, ValueSet> currentValueSetPointerJobMap;
  private Map<JobEntity, ValueSet> lastValueSetPointerJobMap;
  public static final long MINIMUM_FREE_MEMORY_REQUIRED_TO_BE_AVAILABLE_IN_MBS =
      JobRunnerApplicationContext.getZkJobRunnerConfig().getMinFreeMemoryInMbs();

  long startTime = System.currentTimeMillis();

  /**
   * Parameterized constructor
   * 
   * @param dynamicMarshal
   * @param jobEntities
   * @param outputHHPath
   * @param storeAccess
   * @param primaryDimension
   * @param dataDescription
   * @param context
   * @throws IOException
   */
  public SortedDataRowProcessor(DynamicMarshal dynamicMarshal, List<JobEntity> jobEntities,
      String outputHHPath, StoreAccess storeAccess, int primaryDimension,
      DataDescription dataDescription, ShardingApplicationContext context) throws IOException {
    this.jobEntities = jobEntities;
    this.dynamicMarshal = dynamicMarshal;
    this.storeAccess = storeAccess;
    this.executionContext = new ExecutionContextImpl(dynamicMarshal, outputHHPath);
    this.dataDescription = dataDescription;
    this.context = context;
    this.sortedDimensionsOrder = new int[context.getShardingIndexes().length];
    this.orderDimensions(primaryDimension);
    this.buildDataFileAccess();
    this.currentValueSetPointerJobMap = new TreeMap<JobEntity, ValueSet>();
    this.lastValueSetPointerJobMap = new TreeMap<JobEntity, ValueSet>();
    this.setJobFlushPointer();
  }


  @Override
  public void process() {
    LOGGER.info("Sorted dimensions order{}", Arrays.toString(sortedDimensionsOrder));
    LOGGER.info("Number of files opened for job execution{}", pq.size());
    try {
      while (pq.size() > 0) {
        BinaryFileBuffer bfb = pq.poll();
        ByteBuffer row = bfb.pop();
        processRow(row);
        totalNoOfRowsProcessed++;
        logProgress();
        if (bfb.empty()) {
          bfb.getReader().close();
        } else {
          pq.add(bfb);
        }
      }
      flushRemaining();
    } catch (IOException e) {
      LOGGER.error(e.toString());
      throw new RuntimeException(e);
    }
  }

  /**
   * To perform the jobs execution for particular row.
   * 
   * @param row
   */
  private void processRow(ByteBuffer row) {
    for (JobEntity jobEntity : jobEntities) {
      ValueSet valueSet = new ValueSet(jobEntity.getJob().getDimensions());
      updateCurrentValueSetPointer(row, jobEntity);
      ValueSet currentValueSetPointer = currentValueSetPointerJobMap.get(jobEntity);
      boolean jobFlushEligible = prepareValueSet(row, jobEntity, valueSet, currentValueSetPointer);
      Work reducer = prepareReducersBatch(valueSet, jobFlushEligible, jobEntity);
      processReducers(reducer, row);
    }
  }


  /**
   * To prepare the new ValueSet and also determine whether the job is ready to be flushed
   * 
   * @param row
   * @param jobEntity
   * @param valueSet
   * @param currentValueSetPointer
   * @return true if the job is ready to be flushed in output result otherwise false.
   */
  private boolean prepareValueSet(ByteBuffer row, JobEntity jobEntity, ValueSet valueSet,
      ValueSet currentValueSetPointer) {
    boolean jobFlushEligible = false;
    for (int i = 0; i < jobEntity.getJob().getDimensions().length; i++) {
      Object value = dynamicMarshal.readValue(jobEntity.getJob().getDimensions()[i], row);
      valueSet.setValue(value, i);
    }
    if (lastValueSetPointerJobMap.get(jobEntity) == null) {
      ValueSet lastValueSet = new ValueSet(jobEntity.getFlushPointer());
      for (int i = 0; i < jobEntity.getFlushPointer().length; i++) {
        Object value = dynamicMarshal.readValue(jobEntity.getFlushPointer()[i], row);
        lastValueSet.setValue(value, i);
      }
      lastValueSetPointerJobMap.put(jobEntity, lastValueSet);
    } else {
      ValueSet lastValueSet = lastValueSetPointerJobMap.get(jobEntity);
      if (lastValueSet.compareTo(currentValueSetPointer) != 0) {
        jobFlushEligible = true;
        updateLastValueSet(row, jobEntity);
      }
    }
    return jobFlushEligible;
  }


  /**
   * To update the last ValueSet pointer with current ValueSet pointer
   * 
   * @param row
   * @param jobEntity
   */
  private void updateLastValueSet(ByteBuffer row, JobEntity jobEntity) {
    ValueSet lastValueSet = lastValueSetPointerJobMap.get(jobEntity);
    ValueSet currentValueSet = currentValueSetPointerJobMap.get(jobEntity);
    copyValueSet(currentValueSet, lastValueSet);
  }


  /**
   * To update the current ValueSet pointer.
   * 
   * @param row
   * @param jobEntity
   */
  private void updateCurrentValueSetPointer(ByteBuffer row, JobEntity jobEntity) {
    ValueSet currentValueSet = currentValueSetPointerJobMap.get(jobEntity);
    if (currentValueSet == null) {
      currentValueSet = new ValueSet(jobEntity.getFlushPointer());
      for (int i = 0; i < jobEntity.getFlushPointer().length; i++) {
        Object value = dynamicMarshal.readValue(jobEntity.getFlushPointer()[i], row);
        currentValueSet.setValue(value, i);
      }
      currentValueSetPointerJobMap.put(jobEntity, currentValueSet);
    } else {
      ValueSet newValueSet = currentValueSetPointerJobMap.get(jobEntity);
      for (int i = 0; i < jobEntity.getFlushPointer().length; i++) {
        Object value = dynamicMarshal.readValue(jobEntity.getFlushPointer()[i], row);
        newValueSet.setValue(value, i);
      }
    }
  }

  /**
   * To get the reduce for ValueSet
   * 
   * @param valueSet
   * @param isJobFlushable
   * @param jobEntity
   * @return
   */
  private Work prepareReducersBatch(ValueSet valueSet, boolean isJobFlushable,
      JobEntity jobEntity) {
    Work reducer = null;
    reducer = addReducer(valueSet, isJobFlushable, jobEntity);
    return reducer;
  }

  /**
   * To log the progress after particular number of row read.
   */
  private void logProgress() {
    if (totalNoOfRowsProcessed % MAXIMUM_NO_OF_ROWS_TO_LOG_PROGRESS_AFTER == 0) {
      LOGGER.info(
          "Memory status (in MBs): Max free memory available-{}, Used memory-{} ,Total Memory-{}, Free memory {},Max memory-{}.",
          new Object[] {MemoryStatus.getMaximumFreeMemoryThatCanBeAllocated(),
              MemoryStatus.getUsedMemory(), MemoryStatus.getTotalmemory(),
              MemoryStatus.getFreeMemory(), MemoryStatus.getMaxMemory()});
      System.gc();
    }
  }

  /**
   * To process the reducer
   * 
   * @param work
   * @param row
   */
  private void processReducers(Work work, ByteBuffer row) {
    if (work != null) {
      executionContext.setData(row);
      work.processRow(executionContext);
    }
  }

  /**
   * Add the new reduce if not present for particular job otherwise return the existing one.
   * 
   * @param valueSet
   * @param jobFlushEligible
   * @param jobEntity
   * @return reducer
   */
  private Work addReducer(ValueSet valueSet, boolean jobFlushEligible, JobEntity jobEntity) {
    TreeMap<ValueSet, Work> valuesetToWorkTreeMap = jobToValuesetWorkMap.get(jobEntity);
    if (valuesetToWorkTreeMap == null) {
      valuesetToWorkTreeMap = new TreeMap<ValueSet, Work>();
      jobToValuesetWorkMap.put(jobEntity, valuesetToWorkTreeMap);
    }
    if (jobFlushEligible && !valuesetToWorkTreeMap.isEmpty()) {
      finishUp(valuesetToWorkTreeMap);
      valuesetToWorkTreeMap.clear();
    }
    Work work = valuesetToWorkTreeMap.get(valueSet);
    if (work == null) {
      work = jobEntity.getJob().createNewWork();
      valuesetToWorkTreeMap.put(valueSet, work); // put new value set
    }
    return work;
  }

  /**
   * To finish up the reducers.
   * 
   * @param valuesetToWorkTreeMap
   */
  private void finishUp(TreeMap<ValueSet, Work> valuesetToWorkTreeMap) {
    for (Entry<ValueSet, Work> e : valuesetToWorkTreeMap.entrySet()) {
      executionContext.setKeys(e.getKey());
      e.getValue().calculate(executionContext);
    }
    executionContext.flush();
  }

  /**
   * Build the data file and add to the priority queue according to the sorted(ascending) order.
   * 
   * @throws IOException
   */
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

  /**
   * To set the each job's flush pointer.
   */
  private void setJobFlushPointer() {
    List<Integer> dimns = new ArrayList<>();
    for (JobEntity jobEntity : jobEntities) {
      for (int sortDim = 0; sortDim < sortedDimensionsOrder.length; sortDim++) {
        for (int index = 0; index < jobEntity.getJob().getDimensions().length; index++) {
          if (jobEntity.getJob().getDimensions()[index] == sortedDimensionsOrder[sortDim]) {
            dimns.add(sortedDimensionsOrder[sortDim]);
          }
        }
      }
      jobEntity.setFlushPointer(dimns.stream().mapToInt(i -> i).toArray());
      dimns.clear();
    }
    LOGGER.info("All recent jobs {}", Arrays.toString(jobEntities.toArray()));
  }


  private int columnPos = 0;

  /**
   * @param row1
   * @param row2
   * @return return negative inetger value if row1 < row2, zero if row1 = row2, and positive integer
   *         value if row1 > row2
   */
  private int compareRow(byte[] row1, byte[] row2) {
    int res = 0;
    for (int dim = 0; dim < sortedDimensionsOrder.length; dim++) {
      DataLocator locator = dataDescription.locateField(sortedDimensionsOrder[dim]);
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

  /**
   * Instantiate the priority queue for data input stream of files.
   */
  private PriorityQueue<BinaryFileBuffer> pq =
      new PriorityQueue<>(11, new Comparator<BinaryFileBuffer>() {
        @Override
        public int compare(BinaryFileBuffer i, BinaryFileBuffer j) {
          return compareRow(i.peek().array(), j.peek().array());
        }
      });

  /**
   * To arrange the dimensions in sorted order
   * 
   * @param primaryDimension
   */
  private void orderDimensions(int primaryDimension) {
    for (int i = 0; i < context.getShardingIndexes().length; i++) {
      sortedDimensionsOrder[i] = context.getShardingIndexes()[(i + primaryDimension)
          % context.getShardingIndexes().length];
    }
  }

  /**
   * To flush the jobs for their remaining reduces.
   */
  private void flushRemaining() {
    for (JobEntity jobEntity : jobEntities) {
      TreeMap<ValueSet, Work> valuesetToWorkTreeMap = jobToValuesetWorkMap.get(jobEntity);
      if ((valuesetToWorkTreeMap != null) && !valuesetToWorkTreeMap.isEmpty()) {
        LOGGER.info("Flusing for job id {}", jobEntity.getJobId());
        finishUp(valuesetToWorkTreeMap);
        valuesetToWorkTreeMap.clear();
      }
    }
  }

  /**
   * To copy the value set from one ValueSet to other ValueSet.
   * 
   * @param vsFrom
   * @param vsTo
   */
  public void copyValueSet(ValueSet vsFrom, ValueSet vsTo) {
    for (int i = 0; i < vsFrom.getKeyIndexes().length; i++) {
      vsTo.setValue(vsFrom.getValues()[i], i);
    }
  }
}
