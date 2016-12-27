package com.talentica.hdfs.spark.binary.job;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.rdd.job.Job;

public abstract class JobExecutor implements Serializable {
  private static final long serialVersionUID = -2842102492773036355L;

  protected static Logger LOGGER = LoggerFactory.getLogger(JobExecutor.class);
  protected String outputDir;
  protected String distrFile;
  protected String masterIp;
  protected String appName;
  protected String shardingFolderPath;

  public JobExecutor(String[] args) {
    validateProgramArgument(args);
    this.masterIp = args[0];
    this.appName = args[1];
    this.distrFile = args[2];
    this.outputDir = args[3];
    this.shardingFolderPath = args[4];
  }

  public abstract void startJob(JavaSparkContext context, JavaRDD<byte[]> rdd,
      DataDescriptionConfig dataDescriptionConfig, Broadcast<Job> broadcastJob);

  public String getOutputDir() {
    return outputDir;
  }

  public String getDistrFile() {
    return distrFile;
  }

  public String getMasterIp() {
    return masterIp;
  }

  public String getAppName() {
    return appName;
  }

  public String getShardingFolderPath() {
    return shardingFolderPath;
  }

  protected void validateProgramArgument(String[] args) {
    if (args.length < 4) {
      System.err.println(
          "Improper arguments. Please provide in  proper order. i.e <spark-master-ip> <application-name> <distributed-file> <ouput-dir-name>");
      System.out.println(
          "Parameter argumes should be {spark://{master}:7077} {test-app} {hdfs://master-ip:port/distr/data} {output}");
      System.exit(1);
    }
  }

  public void stop(JavaSparkContext context) {
    context.stop();
  }

}
