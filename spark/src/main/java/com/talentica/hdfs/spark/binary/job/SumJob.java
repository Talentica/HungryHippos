package com.talentica.hdfs.spark.binary.job;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.job.JobMatrix;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;

public class SumJob {

  private static JavaSparkContext context;
  private static JobExecutor executor;
  
  public static void main(String[] args){
    executor = new JobExecutor(args);
    DataDescriptionConfig dataDescriptionConfig = new DataDescriptionConfig(executor.getShardingFolderPath());
    initSparkContext();
    JavaRDD<byte[]> rdd = context.binaryRecords(executor.getDistrFile(), dataDescriptionConfig.getRowSize());
    HHRDDRowReader reader = new HHRDDRowReader(dataDescriptionConfig.getDataDescription());
    for(Job job : getSumJobMatrix().getJobs()){
      Broadcast<Job> broadcastJob = context.broadcast(job);
      executor.startJob(context,rdd,reader,dataDescriptionConfig,broadcastJob);
    }
    executor.stop(context);
  }
  
  private static void initSparkContext(){
    if(context == null){
      SparkConf conf = new SparkConf().setMaster(executor.getMasterIp()).setAppName(executor.getAppName());
      context = new JavaSparkContext(conf);
    }
  }
  
  private static JobMatrix getSumJobMatrix() {
    int count = 0;
    JobMatrix sumJobMatrix = new JobMatrix();
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
}
