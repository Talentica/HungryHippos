package com.talentica.hdfs.spark.binary.job;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.job.JobMatrixInterface;

public class SumJob {

  private static JavaSparkContext context;
  private static JobExecutor executor;
  
  public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException{
    executor = new SumJobExecutor(args);
    DataDescriptionConfig dataDescriptionConfig = new DataDescriptionConfig(executor.getShardingFolderPath());
    initSparkContext();
    JavaRDD<byte[]> rdd = context.binaryRecords(executor.getDistrFile(), dataDescriptionConfig.getRowSize());
    for(Job job : getSumJobMatrix()){
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
  
  private static List<Job> getSumJobMatrix() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    /*JobMatrix medianJobMatrix = new JobMatrix();
    medianJobMatrix.addJob(new Job(new Integer[] {0,1},6,0));
    return medianJobMatrix;*/
    Class jobMatrix = Class.forName("com.talentica.hungryHippos.rdd.job.JobMatrix");
    JobMatrixInterface obj =  (JobMatrixInterface) jobMatrix.newInstance();
    obj.printMatrix();
    return obj.getJobs();
  }
}
