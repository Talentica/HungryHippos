/**
 * 
 */
package com.talentica.hungryHippos.rdd.main;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import com.talentica.hdfs.spark.binary.job.JobMatrixInterface;
import com.talentica.hungryHippos.rdd.HHRDDConfiguration;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;
import com.talentica.spark.job.executor.SumJobExecutor;

public class SumJob implements Serializable {

  private static final long serialVersionUID = 8326979063332184463L;
  private static JavaSparkContext context;
  private static SumJobExecutor executor;

  public static void main(String[] args) throws FileNotFoundException, JAXBException, ClassNotFoundException, InstantiationException, IllegalAccessException {
    executor = new SumJobExecutor(args);
    HHRDDConfiguration hhrddConfiguration = new HHRDDConfiguration(executor.getDistrDir(),
        executor.getClientConf(), executor.getOutputFile());
    initializeSparkContext();
    for (Job job : getSumJobMatrix()) {
      Broadcast<Job> jobBroadcast = context.broadcast(job);
      int jobPrimDim = HHRDDHelper
          .getPrimaryDimensionIndexToRunJobWith(job, hhrddConfiguration.getShardingIndexes());
      executor.startSumJob(context, hhrddConfiguration, jobBroadcast,jobPrimDim);
    }
    executor.stop(context);
  }


  private static List<Job> getSumJobMatrix() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    Class jobMatrix = Class.forName("com.talentica.hungryHippos.rdd.job.JobMatrix");
    JobMatrixInterface obj =  (JobMatrixInterface) jobMatrix.newInstance();
    obj.printMatrix();
    return obj.getJobs();
  }

  private static void initializeSparkContext() {
    if (SumJob.context == null) {
      SparkConf conf =
          new SparkConf().setMaster(executor.getMasterIp()).setAppName(executor.getAppName());
      try {
        SumJob.context = new JavaSparkContext(conf);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
