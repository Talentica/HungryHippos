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
/**
 * 
 */
package com.talentica.hungryHippos.rdd.main;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.rdd.HHSparkContext;
import com.talentica.hungryHippos.rdd.main.job.Job;
import com.talentica.hungryHippos.rdd.main.job.JobMatrix;
import com.talentica.hungryHippos.rdd.utility.HHRDDFileUtils;
import com.talentica.spark.job.executor.SumJobExecutorWithShuffle;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SumJobWithShuffle extends AbstractJob {

  private static final long serialVersionUID = 4179843058744172764L;
  private static Logger LOGGER = LoggerFactory.getLogger(SumJobWithShuffle.class);

  public static void main(String[] args) throws IOException, JAXBException, ClassNotFoundException, InstantiationException, IllegalAccessException {
    validateProgramArgument(args);
    String masterIp = args[0];
    String appName = args[1];
    String hhFilePath = args[2];
    String clientConfigPath = args[3];
    String outputDirectory = args[4];
    SumJobWithShuffle jobWithShuffle = new SumJobWithShuffle();
    HHSparkContext context =
        jobWithShuffle.initializeSparkContext(masterIp, appName, clientConfigPath);
    Map<String, JavaRDD<byte[]>> cacheRDD = new HashMap<>();
    Broadcast<DataDescription> descriptionBroadcast =
        context.broadcastFieldDataDescription(hhFilePath);
    for (Job job : getSumJobMatrix().getJobs()) {
      Broadcast<Job> jobBroadcast = context.broadcast(job);
      String keyOfHHRDD = generateKeyForHHRDD(job, context.getShardingIndexes(hhFilePath));
      JavaRDD<byte[]> hipposRDD = cacheRDD.get(keyOfHHRDD);
      if (hipposRDD == null) {
        hipposRDD = context.binaryRecords(job.getDimensions(), hhFilePath, true);
        cacheRDD.put(keyOfHHRDD, hipposRDD);
      }
      JavaPairRDD<String, Long> resultRDD =
          SumJobExecutorWithShuffle.process(hipposRDD, descriptionBroadcast, jobBroadcast);
      String outputDistributedPath = outputDirectory + File.separator + job.getJobId();
      String outputActualPath = context.getActualPath(outputDistributedPath);
      HHRDDFileUtils.saveAsText(resultRDD, outputActualPath);
      LOGGER.info("Output files are in directory {}", outputActualPath);
    }
    context.stop();
  }


  private static JobMatrix getSumJobMatrix() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    JobMatrix sumJobMatrix = new JobMatrix();
    int count = 0;
    for (int i = 0; i < 3; i++) {
      sumJobMatrix.addJob(new Job(new Integer[] {i}, 4, count++));
      sumJobMatrix.addJob(new Job(new Integer[] {i}, 5, count++));
      for (int j = i + 1; j < 4; j++) {
        sumJobMatrix.addJob(new Job(new Integer[] {i, j}, 4, count++));
        sumJobMatrix.addJob(new Job(new Integer[] {i, j}, 5, count++));
        for (int k = j + 1; k < 4; k++) {
          sumJobMatrix.addJob(new Job(new Integer[] {i, j, k}, 4, count++));
          sumJobMatrix.addJob(new Job(new Integer[] {i, j, k}, 5, count++));
        }
      }
    }
    return sumJobMatrix;
  }

  private static void validateProgramArgument(String args[]) {
    if (args.length < 5) {
      System.err.println(
              "Improper arguments. Please provide in  proper order. i.e <spark-master-ip> <application-name> <distributed-directory> <client-configuration> <ouput-file-name>");
      System.out.println(
              "Parameter argumes should be {spark://{master}:7077} {test-app} {/distr/data} {{client-path}/client-config.xml} {output}");
      System.exit(1);
    }
  }
}
