/**
 * 
 */
package com.talentica.hungryHippos.rdd;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkContext;

import com.talentica.hungryHippos.common.job.PrimaryDimensionwiseJobsCollection;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.utility.JobEntity;

/**
 * @author pooshans
 *
 */
public class HungryHipposRDD extends HungryHipposRDDBuilder {

  private List<JobEntity> jobEntities;
  private HungryHipposRDDBuilder hungryHipposRDD;

  public HungryHipposRDD(SparkContext sc, List<JobEntity> jobEntities) {
    super(sc);
    this.jobEntities = jobEntities;
    this.hungryHipposRDD = buildRDD(jobEntities, null);
  }

  public HungryHipposRDDBuilder getHungryHipposRDD() {
    return hungryHipposRDD;
  }

  private HungryHipposRDDBuilder buildRDDbyJobsPrimaryDimension(
      PrimaryDimensionwiseJobsCollection jobSCollection) {
    for (JobEntity jobEntity : jobSCollection.getJobs()) {

    }
    return null;
  }

  private HungryHipposRDDBuilder buildRDD(List<JobEntity> jobEntities,
      ShardingApplicationContext context) {
    List<PrimaryDimensionwiseJobsCollection> jobsCollectionList =
        PrimaryDimensionwiseJobsCollection.from(jobEntities, context);
    HungryHipposRDDBuilder finalHHRDDBuilder;
    List<HungryHipposRDDBuilder> rddBuilder = new ArrayList<HungryHipposRDDBuilder>();
    for (PrimaryDimensionwiseJobsCollection jobSCollection : jobsCollectionList) {
      rddBuilder.add(buildRDDbyJobsPrimaryDimension(jobSCollection));
    }
    finalHHRDDBuilder = rddBuilder.get(0);
    if (rddBuilder.size() > 1) {
      for (int index = 1; index < rddBuilder.size(); index++) {
        finalHHRDDBuilder.union(rddBuilder.get(index));
      }
    }
    return finalHHRDDBuilder;
  }

}
