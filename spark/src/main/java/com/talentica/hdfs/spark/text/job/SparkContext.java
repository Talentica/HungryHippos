package com.talentica.hdfs.spark.text.job;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkContext {

  private JavaSparkContext context;
  
  private  String outputDir;
  private String distrFile;
  private String masterIp;
  private String appName;
  
  public SparkContext(String[] args){
    validateProgramArgument(args);
    this.masterIp = args[0];
    this.appName = args[1];
    this.distrFile = args[2];
    this.outputDir = args[3];
  }
  
  public JavaSparkContext initContext(){
    if(context == null){
      SparkConf conf = new SparkConf().setMaster(masterIp).setAppName(appName);
      context = new JavaSparkContext(conf);
    }
    return context;
  }
  
  public void validateProgramArgument(String[] args){
    if (args.length < 4) {
      System.err.println(
          "Improper arguments. Please provide in  proper order. i.e <spark-master-ip> <application-name> <distributed-file> <ouput-dir-name> <sharding-config-folder>");
      System.out.println(
          "Parameter argumes should be {spark://{master}:7077} {test-app} {hdfs://master-ip:port/distr/data} {output}");
      System.exit(1);
    }
  }

  public JavaSparkContext getContext() {
    return context;
  }

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
  
}
