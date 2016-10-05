package com.talentica.hungryHippos.common.job;

import java.util.ArrayList;
import java.util.Arrays;
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
  public void testFrom1() {
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

  @Test
  public void testFrom2() {
    Mockito.when(context.getShardingIndexes()).thenReturn(new int[] {0, 1, 2});
    List<JobEntity> jobs = new ArrayList<>();
    for (int i = 0; i < 1; i++) {
      jobs.add(new JobEntity(new TestJob(new int[] {i}, 6)));
      jobs.add(new JobEntity(new TestJob(new int[] {i}, 7)));
      for (int j = i + 1; j < 4; j++) {
        jobs.add(new JobEntity(new TestJob(new int[] {i, j}, 6)));
        jobs.add(new JobEntity(new TestJob(new int[] {i, j}, 7)));
        for (int k = j + 1; k < 4; k++) {
          jobs.add(new JobEntity(new TestJob(new int[] {i, j, k}, 6)));
          jobs.add(new JobEntity(new TestJob(new int[] {i, j, k}, 7)));
        }
      }
    }
    // System.out.println("Jobs size:" + jobs.size());
    // jobs.stream().forEach(System.out::println);
    List<PrimaryDimensionwiseJobsCollection> jobsCollection =
        PrimaryDimensionwiseJobsCollection.from(jobs, context);
    Assert.assertNotNull(jobsCollection);
    int totalCountOfJobs = 0;
    for (PrimaryDimensionwiseJobsCollection dimwiseJob : jobsCollection) {
      totalCountOfJobs = totalCountOfJobs + dimwiseJob.getNumberOfJobs();
    }
    Assert.assertEquals(jobs.size(), totalCountOfJobs);
  }

  public static class TestJob implements Job {

    private int[] dimensions;

    private int valueIndex;

    public TestJob(int[] dimensions) {
      this.dimensions = dimensions;
    }

    public TestJob(int[] dimensions, int valueIndex) {
      this(dimensions);
      this.valueIndex = valueIndex;
    }

    @Override
    public Work createNewWork() {
      return null;
    }

    @Override
    public int[] getDimensions() {
      return dimensions;
    }

    @Override
    public String toString() {
      return "TestJob{Dimensions:" + Arrays.toString(dimensions) + ",valueIndex:" + valueIndex
          + "}";
    }

  }

}
