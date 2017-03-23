/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
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
package com.talentica.hungryHippos.rdd.main;

import com.talentica.hungryHippos.rdd.HHSparkContext;
import com.talentica.hungryHippos.rdd.main.job.Job;
import org.apache.spark.SparkConf;

import java.io.Serializable;


public class AbstractJob implements Serializable {

  private static final long serialVersionUID = 3773828590435553782L;

  protected HHSparkContext context;

  protected HHSparkContext initializeSparkContext(String masterIp, String appName,
      String clientConfigPath) {
    if (context == null) {
      SparkConf conf = new SparkConf().setMaster(masterIp).setAppName(appName);
      try {
        context = new HHSparkContext(conf, clientConfigPath);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return context;
  }

  protected static String generateKeyForHHRDD(Job job, int[] sortedShardingIndexes) {
    boolean keyCreated = false;
    Integer[] jobDimensions = job.getDimensions();
    StringBuilder jobShardingDimensions = new StringBuilder();
    for (int i = 0; i < sortedShardingIndexes.length; i++) {
      for (int j = 0; j < jobDimensions.length; j++) {
        if (jobDimensions[j] == sortedShardingIndexes[i]) {
          keyCreated = true;
          jobShardingDimensions.append(sortedShardingIndexes[i]).append("-");
        }
      }
    }
    if (!keyCreated) {
      jobShardingDimensions.append(sortedShardingIndexes[0]);
    }
    return jobShardingDimensions.toString();
  }

}
