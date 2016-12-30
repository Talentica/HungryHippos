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

import com.talentica.hungryHippos.rdd.HHRDDConfiguration;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.job.JobMatrix;
import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;
import com.talentica.spark.job.executor.UniqueCountJobExecutor;

/**
 * @author sudarshans
 *
 */
public class UniqueCountJob implements Serializable{

  private static final long serialVersionUID = -7856558981966923544L;
  private static JavaSparkContext context;
  private static UniqueCountJobExecutor executor;

  public static void main(String[] args) throws FileNotFoundException, JAXBException, ClassNotFoundException, InstantiationException, IllegalAccessException {
    executor = new UniqueCountJobExecutor(args);
    HHRDDConfiguration hhrddConfiguration = new HHRDDConfiguration(executor.getDistrDir(),
        executor.getClientConf(), executor.getOutputFile());
    initializeSparkContext();
    for (Job job : getUniqueCountJobMatrix().getJobs()) {
      Broadcast<Job> jobBroadcast = context.broadcast(job);
      int jobPrimDim = HHRDDHelper
          .getPrimaryDimensionIndexToRunJobWith(job, hhrddConfiguration.getShardingIndexes());
      executor.startUniqueCountJob(context, hhrddConfiguration, jobBroadcast,jobPrimDim);
    }
    executor.stop(context);
  }


  private static JobMatrix getUniqueCountJobMatrix() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    JobMatrix sumJobMatrix = new JobMatrix();
    sumJobMatrix.addJob(new Job(new Integer[] {0,1},6,0));
    return sumJobMatrix;
  }

  private static void initializeSparkContext() {
    if (UniqueCountJob.context == null) {
      SparkConf conf =
          new SparkConf().setMaster(executor.getMasterIp()).setAppName(executor.getAppName());
      try {
        UniqueCountJob.context = new JavaSparkContext(conf);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  
  

}
