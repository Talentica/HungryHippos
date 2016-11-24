/**
 * 
 */
package com.talentica.hungryHippos.rdd.main;

import java.io.FileNotFoundException;
import java.io.Serializable;

import javax.xml.bind.JAXBException;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.job.JobMatrix;

public class HHRDDMain implements Serializable {

  private static final long serialVersionUID = 8326979063332184463L;
  private static SparkContext context;
  private static HHRDDExecutor executor;

  public static void main(String[] args) throws FileNotFoundException, JAXBException {
    executor = new HHRDDExecutor(args);
    initializeSparkContext();
    executor.startSumJob(context, getSumJobMatrix());
    executor.stop(context);
  }

  private static JobMatrix getSumJobMatrix() {
    int count = 0;
    JobMatrix sumJobMatrix = new JobMatrix();
    for (int i = 0; i < 3; i++) {
      sumJobMatrix.addJob(new Job(new Integer[] {i}, 6, count++));
    }
    return sumJobMatrix;
  }

  @SuppressWarnings("resource")
  private static void initializeSparkContext() {
    if (HHRDDMain.context == null) {
      SparkConf conf =
          new SparkConf().setMaster(executor.getMasterIp()).setAppName(executor.getAppName());
      try {
        HHRDDMain.context = new JavaSparkContext(conf).sc();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
