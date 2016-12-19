/**
 * 
 */
package com.talentica.hungryHippos.rdd.main;

import java.io.FileNotFoundException;
import java.io.Serializable;

import javax.xml.bind.JAXBException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.rdd.HHRDDConfiguration;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.job.JobMatrix;
import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;

public class HHRDDMain implements Serializable {

  private static final long serialVersionUID = 8326979063332184463L;
  private static JavaSparkContext context;
  private static HHRDDExecutor executor;
  private static Logger LOGGER = LoggerFactory.getLogger(HHRDDMain.class);

  public static void main(String[] args) throws FileNotFoundException, JAXBException {
    executor = new HHRDDExecutor(args);
    HHRDDConfiguration hhrddConfiguration = new HHRDDConfiguration(executor.getDistrDir(),
        executor.getClientConf(), executor.getOutputFile());
    initializeSparkContext();
    for (Job job : getSumJobMatrix().getJobs()) {
      Broadcast<Job> jobBroadcast = context.broadcast(job);
      int jobPrimDim = HHRDDHelper
          .getPrimaryDimensionIndexToRunJobWith(job, hhrddConfiguration.getShardingIndexes());
      executor.startSumJob(context, hhrddConfiguration, jobBroadcast,jobPrimDim);
    }
    executor.stop(context);
  }


  private static JobMatrix getSumJobMatrix() {
    int count = 0;
    JobMatrix sumJobMatrix = new JobMatrix();
    for (int i = 0; i < 3; i++) {
      sumJobMatrix.addJob(new Job(new Integer[] {i}, 6, count++));
      sumJobMatrix.addJob(new Job(new Integer[] {i}, 7, count++));
      for (int j = i + 1; j < 4; j++) {
        sumJobMatrix.addJob(new Job(new Integer[] {i, j}, 6, count++));
        sumJobMatrix.addJob(new Job(new Integer[] {i, j}, 7, count++));
        for (int k = j + 1; k < 4; k++) {
          sumJobMatrix.addJob(new Job(new Integer[] {i, j, k}, 6, count++));
          sumJobMatrix.addJob(new Job(new Integer[] {i, j, k}, 7, count++));
        }
      }
    }
    return sumJobMatrix;
  }

  @SuppressWarnings("resource")
  private static void initializeSparkContext() {
    if (HHRDDMain.context == null) {
      SparkConf conf =
          new SparkConf().setMaster(executor.getMasterIp()).setAppName(executor.getAppName());
      try {
        HHRDDMain.context = new JavaSparkContext(conf);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
