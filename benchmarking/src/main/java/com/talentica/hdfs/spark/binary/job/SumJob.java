/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.talentica.hdfs.spark.binary.job;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.rdd.main.job.Job;
import com.talentica.hungryHippos.rdd.main.job.JobMatrix;
import com.talentica.spark.job.executor.SumJobExecutorWithShuffle;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.FileNotFoundException;

public class SumJob {

  private static JavaSparkContext context;
  protected static Logger LOGGER = LoggerFactory.getLogger(SumJob.class);

  public static void main(String[] args) throws JAXBException, FileNotFoundException {
    String masterIp = args[0];
    String appName = args[1];
    String inputFile = args[2];
    String outputDir = args[3];
    String shardingFolderPath = args[4];

    initSparkContext(masterIp, appName);

    DataDescriptionConfig dataDescriptionConfig = new DataDescriptionConfig(shardingFolderPath);
    JavaRDD<byte[]> rdd = context.binaryRecords(inputFile, dataDescriptionConfig.getRowSize());
    Broadcast<DataDescription> dataDes =
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
