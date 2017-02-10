/**
 * 
 */
package com.talentica.hungryHippos.rdd.main;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.rdd.HHBinaryRDD;
import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.HHRDDInfo;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.job.JobMatrix;
import com.talentica.hungryHippos.rdd.utility.HHRDDFileUtils;
import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;
import com.talentica.spark.job.executor.SumJobExecutorWithShuffle;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

public class SumJobWithShuffle {

  private static Logger LOGGER = LoggerFactory.getLogger(SumJobWithShuffle.class);
  private static JavaSparkContext context;

  public static void main(String[] args) throws FileNotFoundException, JAXBException, ClassNotFoundException, InstantiationException, IllegalAccessException {
    validateProgramArgument(args);
    String masterIp = args[0];
    String appName = args[1];
    String hhFilePath = args[2];
    String clientConfigPath = args[3];
    String outputDirectory = args[4];
    initializeSparkContext(masterIp,appName);
    HHRDDHelper.initialize(clientConfigPath);
    Map<String,HHBinaryRDD> cacheRDD = new HashMap<>();
    HHRDDInfo hhrddInfo = HHRDDHelper.getHhrddInfo(hhFilePath);
    Broadcast<FieldTypeArrayDataDescription> descriptionBroadcast =
            context.broadcast(hhrddInfo.getFieldDataDesc());
    for (Job job : getSumJobMatrix().getJobs()) {
      Broadcast<Job> jobBroadcast = context.broadcast(job);
      String keyOfHHRDD = HHRDDHelper.generateKeyForHHRDD(job, hhrddInfo.getShardingIndexes());
      HHBinaryRDD hipposRDD = cacheRDD.get(keyOfHHRDD);
      if (hipposRDD == null) {
        hipposRDD = new HHBinaryRDD(context, hhrddInfo,job.getDimensions(),true);
        cacheRDD.put(keyOfHHRDD, hipposRDD);
      }
      JavaPairRDD<String, Long> resultRDD=  SumJobExecutorWithShuffle.process(hipposRDD,descriptionBroadcast,jobBroadcast);
      String outputDistributedPath = outputDirectory + File.separator + job.getJobId();

      String outputActualPath =  HHRDDHelper.getActualPath(outputDistributedPath);
      HHRDDFileUtils.saveAsText(resultRDD, outputActualPath);
      LOGGER.info("Output files are in directory {}", outputActualPath);
    }
    context.stop();
  }


  private static JobMatrix getSumJobMatrix() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    JobMatrix sumJobMatrix = new JobMatrix();
    int count = 0;
    for (int i = 0; i < 3; i++) {
      sumJobMatrix.addJob(new Job(new Integer[] {i}, 5, count++));
      sumJobMatrix.addJob(new Job(new Integer[] {i}, 6, count++));
      for (int j = i + 1; j < 4; j++) {
        sumJobMatrix.addJob(new Job(new Integer[] {i, j}, 5, count++));
        sumJobMatrix.addJob(new Job(new Integer[] {i, j}, 6, count++));
        for (int k = j + 1; k < 4; k++) {
          sumJobMatrix.addJob(new Job(new Integer[] {i, j, k}, 5, count++));
          sumJobMatrix.addJob(new Job(new Integer[] {i, j, k}, 6, count++));
        }
      }
    }
    return sumJobMatrix;
  }

  private static void initializeSparkContext(String masterIp, String appName) {
    if (SumJobWithShuffle.context == null) {
      SparkConf conf =
          new SparkConf().setMaster(masterIp).setAppName(appName);
      try {
        SumJobWithShuffle.context = new JavaSparkContext(conf);
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
