package com.talentica.hdfs.spark.binary.job;

import java.nio.ByteBuffer;

import com.talentica.hungryHippos.rdd.main.SumJobWithShuffle;
import com.talentica.spark.job.executor.SumJobExecutorWithShuffle;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.job.JobMatrix;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;

import scala.Tuple2;

public class SumJob {

  private static JavaSparkContext context;
  protected static Logger LOGGER = LoggerFactory.getLogger(SumJob.class);

  public static void main(String[] args) {
    String masterIp = args[0];
    String appName = args[1];
    String inputFile = args[2];
    String outputDir = args[3];
    String shardingFolderPath = args[4];

    initSparkContext(masterIp, appName);

    DataDescriptionConfig dataDescriptionConfig = new DataDescriptionConfig(shardingFolderPath);
    JavaRDD<byte[]> rdd = context.binaryRecords(inputFile, dataDescriptionConfig.getRowSize());
    Broadcast<FieldTypeArrayDataDescription> dataDes =
        context.broadcast(dataDescriptionConfig.getDataDescription());
    for (Job job : getSumJobMatrix().getJobs()) {
      Broadcast<Job> broadcastJob = context.broadcast(job);
        JavaPairRDD<String, Long> pairRDD = SumJobExecutorWithShuffle.process(rdd, dataDes, broadcastJob);
        pairRDD.saveAsTextFile(outputDir + broadcastJob.value().getJobId());
        LOGGER.info("Output files are in directory {}", outputDir + broadcastJob.value().getJobId());
    }
    context.stop();
  }

  private static void initSparkContext(String masterIp, String appName) {
    if (context == null) {
      SparkConf conf = new SparkConf().setMaster(masterIp).setAppName(appName);
      context = new JavaSparkContext(conf);
    }
  }

  private static JobMatrix getSumJobMatrix() {
    JobMatrix sumJobMatrix = new JobMatrix();
    int count = 0;

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
