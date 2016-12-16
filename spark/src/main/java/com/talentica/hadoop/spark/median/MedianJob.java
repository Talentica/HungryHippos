package com.talentica.hadoop.spark.median;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import com.talentica.hadoop.spark.job.DataDescriptionConfig;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.job.JobMatrix;

public class MedianJob {

  private static JavaSparkContext context;
  private static MedianJobExecutor executor;
  
  public static void main(String[] args){
    executor = new MedianJobExecutor(args);
    DataDescriptionConfig dataDescriptionConfig = new DataDescriptionConfig(executor.getShardingFolderPath());
    initSparkContext();
    for(Job job : getSumJobMatrix().getJobs()){
      Broadcast<Job> broadcastJob = context.broadcast(job);
      executor.startJob(context,dataDescriptionConfig,broadcastJob);
    }
  }
  
  private static void initSparkContext(){
    if(context == null){
      SparkConf conf = new SparkConf().setMaster(executor.getMasterIp()).setAppName(executor.getAppName());
      context = new JavaSparkContext(conf);
    }
  }
  
  private static JobMatrix getSumJobMatrix() {
    JobMatrix medianJobMatrix = new JobMatrix();
    medianJobMatrix.addJob(new Job(new Integer[] {0},6,0));
    return medianJobMatrix;
  }
  
}
