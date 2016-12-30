package com.talentica.hdfs.spark.binary.job;

import java.io.File;
import java.io.FileNotFoundException;

import javax.xml.bind.JAXBException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.HHRDDConfigSerialized;
import com.talentica.hungryHippos.rdd.CustomHHJobConfiguration;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;

public class UniqueCounterJob {

  private static JavaSparkContext context;

  public static void main(String[] args) throws ClassNotFoundException, InstantiationException,
      IllegalAccessException, FileNotFoundException, JAXBException {
    validateArgsAndInitializeSparkContext(args);
    String inputDataFilePath = args[2];
    String clientConfigFilePath = args[3];
    String outputFilePath = args[4];
    CustomHHJobConfiguration customHHJobConfiguration =
        new CustomHHJobConfiguration(inputDataFilePath, clientConfigFilePath, outputFilePath);
    Job job = new Job(new Integer[] {0}, 8, 0);

    HHRDDConfigSerialized hhrddConfigSerialized = HHRDDHelper.getHhrddConfigSerialized(inputDataFilePath,clientConfigFilePath);
    HHRDD hhrdd = new HHRDD(context, hhrddConfigSerialized,job.getDimensions());
    Broadcast<Job> broadcastJob = context.broadcast(job);
    DataDescriptionConfig dataDescriptionConfig =
        new DataDescriptionConfig(FileSystemContext.getRootDirectory() + inputDataFilePath
            + File.separatorChar + "sharding-table" + File.separatorChar);
    new UniqueCounterJobExecutor(customHHJobConfiguration.getOutputFileName() + File.separator)
        .startJob(context, hhrdd.toJavaRDD(), dataDescriptionConfig, broadcastJob);
    context.stop();
  }

  private static void validateArgsAndInitializeSparkContext(String[] args) {
    if (args.length != 5) {
      throw new IllegalArgumentException("Required parameters to unique counter job missing. Only "
          + args.length
          + " arguments passed. Please pass following program args: <master ip> <application name> <distributed data file path to run job on> <client-config xml location> <output file name>");
    }
    if (context == null) {
      String appName = args[1];
      String masterIp = args[0];
      SparkConf conf = new SparkConf().setMaster(masterIp).setAppName(appName);
      context = new JavaSparkContext(conf);
    }
  }
}
