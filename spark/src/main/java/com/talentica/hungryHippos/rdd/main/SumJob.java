/**
 * 
 */
package com.talentica.hungryHippos.rdd.main;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.HHRDDConfigSerialized;
import com.talentica.hungryHippos.rdd.RDDBuilder;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.job.JobMatrix;
import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;
import com.talentica.spark.job.executor.SumJobExecutor;

public class SumJob implements Serializable {

  private static final long serialVersionUID = 8326979063332184463L;
  private static JavaSparkContext context;

  public static void main(String[] args) throws FileNotFoundException, JAXBException, ClassNotFoundException, InstantiationException, IllegalAccessException {
    validateProgramArgument(args);
    String masterIp = args[0];
    String appName = args[1];
    String distrDir = args[2];
    String clientConfigPath = args[3];
    String outputDirectory = args[4];
    initializeSparkContext(masterIp,appName);
    HHRDDHelper.initialize(clientConfigPath);
    SumJobExecutor executor = new SumJobExecutor();
    Map<String,HHRDD> cacheRDD = new HashMap<>();
    RDDBuilder.initialize(distrDir);
    HHRDDConfigSerialized hhrddConfigSerialized = HHRDDHelper.getHhrddConfigSerialized(distrDir);
    Broadcast<FieldTypeArrayDataDescription> descriptionBroadcast =
            context.broadcast(hhrddConfigSerialized.getFieldTypeArrayDataDescription());
    for (Job job : getSumJobMatrix().getJobs()) {
      HHRDD hipposRDD = RDDBuilder.gerOrCreateRDD(job, context);
      Broadcast<Job> jobBroadcast = context.broadcast(job);
      executor.startSumJob(hipposRDD,descriptionBroadcast,jobBroadcast, outputDirectory);
    }
    executor.stop(context);
  }


  private static JobMatrix getSumJobMatrix() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    JobMatrix sumJobMatrix = new JobMatrix();
    int count = 0;
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

  private static void initializeSparkContext(String masterIp, String appName) {
    if (SumJob.context == null) {
      SparkConf conf =
          new SparkConf().setMaster(masterIp).setAppName(appName);
      try {
        SumJob.context = new JavaSparkContext(conf);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private static void validateProgramArgument(String args[]) {
    if (args.length < 5) {
      System.err.println(
              "Improper arguments. Please provide in  proper order. i.e <spark-master-ip> <application-name> <distributed-directory> <client-configuration> <ouput-file-name>");
      System.out.println(
              "Parameter argumes should be {spark://{master}:7077} {test-app} {/distr/data} {{client-path}/client-config.xml} {output}");
      System.exit(1);
    }
  }
}
