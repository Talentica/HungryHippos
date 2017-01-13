package com.talentica.hdfs.spark.binary.job;

import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.HHRDDInfo;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileNotFoundException;

public class UniqueCounterJob {

  private static JavaSparkContext context;

  public static void main(String[] args) throws ClassNotFoundException, InstantiationException,
      IllegalAccessException, FileNotFoundException, JAXBException {
    validateArgsAndInitializeSparkContext(args);
    String inputDataFilePath = args[2];
    String clientConfigFilePath = args[3];
    String outputFilePath = args[4];
    HHRDDHelper.initialize(clientConfigFilePath);
    Job job = new Job(new Integer[] {0}, 8, 0);
    HHRDDInfo hhrddInfo = HHRDDHelper.getHhrddInfo(inputDataFilePath);
    HHRDD hhrdd = new HHRDD(context, hhrddInfo,job.getDimensions(),false);
    Broadcast<Job> broadcastJob = context.broadcast(job);
    DataDescriptionConfig dataDescriptionConfig =
        new DataDescriptionConfig(FileSystemContext.getRootDirectory() + inputDataFilePath
            + File.separatorChar + "sharding-table" + File.separatorChar);
    new UniqueCounterJobExecutor(outputFilePath + File.separator)
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
