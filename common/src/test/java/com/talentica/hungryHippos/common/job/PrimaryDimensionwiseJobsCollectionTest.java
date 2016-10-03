package com.talentica.hungryHippos.common.job;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.talentica.hungryHippos.client.domain.Work;
import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.utility.JobEntity;

@RunWith(MockitoJUnitRunner.class)
public class PrimaryDimensionwiseJobsCollectionTest {

  @Mock
  private ShardingApplicationContext context;

  @Test
  public void testFrom() {
    Mockito.when(context.getShardingIndexes()).thenReturn(new int[] {0, 1, 4, 6});
    List<JobEntity> jobs = new ArrayList<>();
    jobs.add(new JobEntity(new TestJob(new int[] {3, 10, 0}))); // 0
    jobs.add(new JobEntity(new TestJob(new int[] {4, 7, 2}))); // 4
    jobs.add(new JobEntity(new TestJob(new int[] {4, 6, 2}))); // 6
    jobs.add(new JobEntity(new TestJob(new int[] {1, 6, 0}))); // 6
    jobs.add(new JobEntity(new TestJob(new int[] {0, 1, 6, 9, 10}))); // 6
    jobs.add(new JobEntity(new TestJob(new int[] {1, 2, 3, 10, 5, 9}))); // 1
    jobs.add(new JobEntity(new TestJob(new int[] {1, 8, 9}))); // 1
    List<PrimaryDimensionwiseJobsCollection> dimensionwiseJobsCollection =
        PrimaryDimensionwiseJobsCollection.from(jobs, context);
    Assert.assertNotNull(dimensionwiseJobsCollection);
    Assert.assertEquals(4, dimensionwiseJobsCollection.size());

    PrimaryDimensionwiseJobsCollection primaryDimensionwiseJobsCollectionWithIndex0 =
        new PrimaryDimensionwiseJobsCollection(0);
    int indexOfKey0 =
        dimensionwiseJobsCollection.indexOf(primaryDimensionwiseJobsCollectionWithIndex0);
    Assert.assertNotEquals(-1, indexOfKey0);
    PrimaryDimensionwiseJobsCollection objectWithPrimaryIndex0 =
        dimensionwiseJobsCollection.get(indexOfKey0);
    Assert.assertNotNull(objectWithPrimaryIndex0);
    Assert.assertEquals(1, objectWithPrimaryIndex0.getNumberOfJobs());

    PrimaryDimensionwiseJobsCollection primaryDimensionwiseJobsCollectionWithIndex4 =
        new PrimaryDimensionwiseJobsCollection(4);
    int indexOfKey4 =
        dimensionwiseJobsCollection.indexOf(primaryDimensionwiseJobsCollectionWithIndex4);
    Assert.assertNotEquals(-1, indexOfKey4);
    PrimaryDimensionwiseJobsCollection objectWithPrimaryIndex4 =
        dimensionwiseJobsCollection.get(indexOfKey4);
    Assert.assertNotNull(objectWithPrimaryIndex4);
    Assert.assertEquals(1, objectWithPrimaryIndex4.getNumberOfJobs());


    PrimaryDimensionwiseJobsCollection primaryDimensionwiseJobsCollectionWithIndex6 =
        new PrimaryDimensionwiseJobsCollection(6);
    int indexOfKey6 =
        dimensionwiseJobsCollection.indexOf(primaryDimensionwiseJobsCollectionWithIndex6);
    Assert.assertNotEquals(-1, indexOfKey6);
    PrimaryDimensionwiseJobsCollection objectWithPrimaryIndex6 =
        dimensionwiseJobsCollection.get(indexOfKey6);
    Assert.assertNotNull(objectWithPrimaryIndex6);
    Assert.assertEquals(3, objectWithPrimaryIndex6.getNumberOfJobs());

    PrimaryDimensionwiseJobsCollection primaryDimensionwiseJobsCollectionWithIndex1 =
        new PrimaryDimensionwiseJobsCollection(1);
    int indexOfKey1 =
        dimensionwiseJobsCollection.indexOf(primaryDimensionwiseJobsCollectionWithIndex1);
    Assert.assertNotEquals(-1, indexOfKey1);
    PrimaryDimensionwiseJobsCollection objectWithPrimaryIndex1 =
        dimensionwiseJobsCollection.get(indexOfKey1);
    Assert.assertNotNull(objectWithPrimaryIndex1);
    Assert.assertEquals(2, objectWithPrimaryIndex1.getNumberOfJobs());
  }

  public static class TestJob implements Job {

    private int[] dimensions;

    public TestJob(int[] dimensions) {
      this.dimensions = dimensions;
    }

    @Override
    public Work createNewWork() {
      return null;
    }

    @Override
    public int[] getDimensions() {
      return dimensions;
    }

  }

  public static void main(String[] args) {
    int[] primaryDims = new int[] {1, 1, 1, 1, 1, 0, 1, 1, 1, 0, 1};
    int start = -1;
    int end = -1;
    int lastSum = 0;
    int currentSum = 0;
    int tempStart = -1;
    int tempEnd = -1;
    int initialSum = 0;

    for (int j = 0; j < primaryDims.length; j++) {
      if (primaryDims[j] == 1) {
        if (tempStart == -1) {
          tempStart = j;
        }
        tempEnd = j;
        currentSum++;
      } else {
        if (primaryDims[0] == 1 && initialSum == 0) {
          initialSum = currentSum;
        }
        currentSum = 0;
        tempStart = -1;
        tempEnd = -1;
      }

      if (currentSum >= lastSum) {
        start = tempStart;
        end = tempEnd;
        lastSum = currentSum;
      }

    }
    if (primaryDims[0] == 1 && primaryDims[primaryDims.length - 1] == 1
        && (currentSum + initialSum) >= lastSum) {
      start = tempStart;
      end = initialSum - 1;
    }

    System.out.println("Start:" + start + ", End:" + end);
  }

}
