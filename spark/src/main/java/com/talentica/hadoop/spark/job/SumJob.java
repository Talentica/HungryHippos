package com.talentica.hadoop.spark.job;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.job.JobMatrixInterface;

public class SumJob {

  private static JavaSparkContext context;
  private static JobExecutor executor;
  
  public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException{
    executor = new JobExecutor(args);
    DataDescriptionConfig dataDescriptionConfig = new DataDescriptionConfig(executor.getShardingFolderPath());
    initSparkContext();
    for(Job job : getSumJobMatrix()){
      Broadcast<Job> broadcastJob = context.broadcast(job);
      executor.startJob(context,dataDescriptionConfig,broadcastJob);
    }
    executor.stop(context);
  }
  
  private static void initSparkContext(){
    if(context == null){
      SparkConf conf = new SparkConf().setMaster(executor.getMasterIp()).setAppName(executor.getAppName());
      context = new JavaSparkContext(conf);
    }
  }
  
  private static List<Job> getSumJobMatrix() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    Class jobMatrix = Class.forName("com.talentica.hungryHippos.rdd.job.JobMatrix");
    JobMatrixInterface obj =  (JobMatrixInterface) jobMatrix.newInstance();
    obj.printMatrix();
    return obj.getJobs();
  }
}
