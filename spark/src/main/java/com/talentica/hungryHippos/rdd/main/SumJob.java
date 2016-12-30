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

import com.talentica.hungryHippos.rdd.CustomHHJobConfiguration;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.job.JobMatrix;
import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;
import com.talentica.spark.job.executor.SumJobExecutor;

public class SumJob implements Serializable {

  private static final long serialVersionUID = 8326979063332184463L;
  private static JavaSparkContext context;
  private static SumJobExecutor executor;

  public static void main(String[] args) throws FileNotFoundException, JAXBException, ClassNotFoundException, InstantiationException, IllegalAccessException {
    executor = new SumJobExecutor(args);
    CustomHHJobConfiguration customHHJobConfiguration = new CustomHHJobConfiguration(executor.getDistrDir(),
        executor.getClientConf(), executor.getOutputFile());
    initializeSparkContext();
    for (Job job : getSumJobMatrix().getJobs()) {
      executor.startSumJob(context, customHHJobConfiguration, job);
    }
    executor.stop(context);
  }


  private static JobMatrix getSumJobMatrix() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    JobMatrix sumJobMatrix = new JobMatrix();
    sumJobMatrix.addJob(new Job(new Integer[] {0,1},6,0));
    return sumJobMatrix;
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
