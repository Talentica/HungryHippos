package com.talentica.hdfs.spark.binary.job;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.job.JobMatrix;

public class UniqueCountJob {

  private static JavaSparkContext context;
  private static JobExecutor executor;
  
  public static void main(String[] args){
    executor = new UniqueCountExecutor(args);
    DataDescriptionConfig dataDescriptionConfig = new DataDescriptionConfig(executor.getShardingFolderPath());
    initSparkContext();
    JavaRDD<byte[]> rdd = context.binaryRecords(executor.getDistrFile(), dataDescriptionConfig.getRowSize());
    for(Job job : getSumJobMatrix().getJobs()){
      Broadcast<Job> broadcastJob = context.broadcast(job);
      executor.startJob(context,rdd,dataDescriptionConfig,broadcastJob);
    }
    executor.stop(context);
  }
  
  private static void initSparkContext(){
    if(context == null){
      SparkConf conf = new SparkConf().setMaster(executor.getMasterIp()).setAppName(executor.getAppName());
      context = new JavaSparkContext(conf);
    }
  }
  
  private static JobMatrix getSumJobMatrix(){
    JobMatrix medianJobMatrix = new JobMatrix();
    medianJobMatrix.addJob(new Job(new Integer[] {0,1},6,0));
    return medianJobMatrix;
  }
}
