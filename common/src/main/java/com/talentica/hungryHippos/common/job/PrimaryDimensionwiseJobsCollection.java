package com.talentica.hungryHippos.common.job;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.utility.JobEntity;

public class PrimaryDimensionwiseJobsCollection {

  private int primaryDimensionIndex;

  private List<JobEntity> jobs = new ArrayList<>();

  public PrimaryDimensionwiseJobsCollection(int primaryDimensionIndex) {
    this.primaryDimensionIndex = primaryDimensionIndex;
  }

  public void addJob(JobEntity job) {
    if (IntStream.of(job.getJob().getDimensions())
        .anyMatch(dimension -> dimension == primaryDimensionIndex)) {
      jobs.add(job);
    }
  }

  public JobEntity jobAt(int index) {
    return jobs.get(index);
  }

  public int getNumberOfJobs() {
    return jobs.size();
  }

  public static List<PrimaryDimensionwiseJobsCollection> from(List<JobEntity> jobs,
      ShardingApplicationContext context) {
    List<PrimaryDimensionwiseJobsCollection> jobsCollection = new ArrayList<>();
    int[] shardingIndexes = context.getShardingIndexes();
    Arrays.sort(shardingIndexes);
    jobs.stream().forEach(job -> {
      int primaryDimensionToRunJobWith = getPrimaryDimensionIndexToRunJobWith(job, shardingIndexes);
      Optional<PrimaryDimensionwiseJobsCollection> existingPrimaryDimensionwiseJobsCollectionOptional =
          jobsCollection.stream()
              .filter(
                  primaryDimensionwiseJobsCollection -> primaryDimensionwiseJobsCollection.primaryDimensionIndex == primaryDimensionToRunJobWith)
              .findFirst();
      if (existingPrimaryDimensionwiseJobsCollectionOptional.isPresent()) {
        PrimaryDimensionwiseJobsCollection existingPrimaryDimensionwiseJobsCollection =
            existingPrimaryDimensionwiseJobsCollectionOptional.get();
        existingPrimaryDimensionwiseJobsCollection.addJob(job);
      } else {
        PrimaryDimensionwiseJobsCollection newPrimaryDimensionwiseJobsCollection =
            new PrimaryDimensionwiseJobsCollection(primaryDimensionToRunJobWith);
        jobsCollection.add(newPrimaryDimensionwiseJobsCollection);
        newPrimaryDimensionwiseJobsCollection.addJob(job);
      }
    });
    return jobsCollection;
  }

  private static int getPrimaryDimensionIndexToRunJobWith(JobEntity job,
      int[] sortedShardingIndexes) {
    int[] jobDimensions = job.getJob().getDimensions();
    List<Integer> dimensionsList = new ArrayList<>();
    Arrays.stream(jobDimensions).forEach(value -> dimensionsList.add(value));
    int[] filteredPrimaryOnlyJobDimensions = Arrays.stream(sortedShardingIndexes)
        .filter(shardIndex -> dimensionsList.contains(shardIndex)).toArray();
    return getShardingIndexForJobExecutionToMaximizeUseOfSortedData(
        filteredPrimaryOnlyJobDimensions);
  }

  private static int getShardingIndexForJobExecutionToMaximizeUseOfSortedData(
      int[] primaryOnlyJobDimensions) {
    Arrays.sort(primaryOnlyJobDimensions);
    int maxPrimaryJobDimension = primaryOnlyJobDimensions[primaryOnlyJobDimensions.length - 1];
    int[] dimensions = new int[maxPrimaryJobDimension + 1];
    Arrays.stream(primaryOnlyJobDimensions)
        .forEach(currentDimension -> dimensions[currentDimension] = 1);
    int start = -1;
    int lastSum = 0;
    int currentSum = 0;
    int tempStart = -1;
    int initialSum = 0;

    for (int j = 0; j < dimensions.length; j++) {
      if (dimensions[j] == 1) {
        if (tempStart == -1) {
          tempStart = j;
        }
        currentSum++;
      } else {
        if (dimensions[0] == 1 && initialSum == 0) {
          initialSum = currentSum;
        }
        currentSum = 0;
        tempStart = -1;
      }

      if (currentSum >= lastSum) {
        start = tempStart;
        lastSum = currentSum;
      }

    }
    if (dimensions[0] == 1 && dimensions[dimensions.length - 1] == 1
        && (currentSum + initialSum) >= lastSum) {
      start = tempStart;
    }
    return start;
  }



  @Override
  public int hashCode() {
    return primaryDimensionIndex;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj != null && obj instanceof PrimaryDimensionwiseJobsCollection) {
      PrimaryDimensionwiseJobsCollection other = (PrimaryDimensionwiseJobsCollection) obj;
      return other.primaryDimensionIndex == primaryDimensionIndex;
    }
    return false;
  }

  @Override
  public String toString() {
    return "{Primary Dimension:" + primaryDimensionIndex + ", Jobs Collection Size:"
        + getNumberOfJobs() + "}";
  }

}
