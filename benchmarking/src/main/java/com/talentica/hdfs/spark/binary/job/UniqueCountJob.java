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

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.rdd.main.job.Job;
import com.talentica.hungryHippos.rdd.main.job.JobMatrix;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import javax.xml.bind.JAXBException;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class UniqueCountJob {

  private static JavaSparkContext context;
  protected static Logger LOGGER = LoggerFactory.getLogger(UniqueCountJob.class);
  
  public static void main(String[] args) throws JAXBException, FileNotFoundException {
    String masterIp = args[0];
    String appName = args[1];
    String inputFile = args[2];
    String outputDir = args[3];
    String shardingFolderPath = args[4];
    
    initSparkContext(masterIp,appName);
    
    DataDescriptionConfig dataDescriptionConfig = new DataDescriptionConfig(shardingFolderPath);
    JavaRDD<byte[]> rdd = context.binaryRecords(inputFile, dataDescriptionConfig.getRowSize());
    Broadcast<FieldTypeArrayDataDescription> dataDes =
        context.broadcast(dataDescriptionConfig.getDataDescription());
        
    for(Job job : getSumJobMatrix().getJobs()){
      Broadcast<Job> broadcastJob = context.broadcast(job);
      startJob(rdd,dataDes,broadcastJob,outputDir);
    }
    context.stop();
  }
  
  private static void initSparkContext(String masterIp,String appName){
    if(context == null){
      SparkConf conf = new SparkConf().setMaster(masterIp).setAppName(appName);
      context = new JavaSparkContext(conf);
    }
  }
  
  private static JobMatrix getSumJobMatrix(){
    JobMatrix medianJobMatrix = new JobMatrix();
    medianJobMatrix.addJob(new Job(new Integer[] {0,1},6,0));
    return medianJobMatrix;
  }
  
  public static void startJob(JavaRDD<byte[]> rdd,Broadcast<FieldTypeArrayDataDescription> dataDes,
      Broadcast<Job> broadcastJob,String outputDir) {
    JavaPairRDD<String, Integer> pairRDD = rdd.repartition(2000).mapToPair(new PairFunction<byte[], String, Integer>() {
      private static final long serialVersionUID = -1533590342050196085L;

      @Override
      public Tuple2<String, Integer> call(byte[] buf) throws Exception {
        HHRDDRowReader readerVar = new HHRDDRowReader(dataDes.getValue());
        ByteBuffer byteBuffer = ByteBuffer.wrap(buf);
        readerVar.setByteBuffer(byteBuffer);
        StringBuilder key = new StringBuilder();
        for (int index = 0; index < broadcastJob.value().getDimensions().length; index++) {
          key.append(( readerVar
                  .readAtColumn(broadcastJob.value().getDimensions()[index])).toString());
        }
        key.append("|id=").append(broadcastJob.value().getJobId());
        Integer value = (Integer) readerVar.readAtColumn(broadcastJob.value().getCalculationIndex());
        return new Tuple2<>(key.toString(), value);
      }
    });
    JavaPairRDD<String, Iterable<Integer>> pairRDDGroupedByKey = pairRDD.groupByKey();
    JavaPairRDD<String, Long> result = pairRDDGroupedByKey
        .mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Long>() {
          private static final long serialVersionUID = 484111559311975643L;

          @Override
          public Tuple2<String, Long> call(Tuple2<String, Iterable<Integer>> t) throws Exception {
            HyperLogLog hyperLogLog = new HyperLogLog(0.01);
            Iterator<Integer> itr = t._2.iterator();
            while (itr.hasNext()) {
              hyperLogLog.offer(itr.next());
            }
            Long count = hyperLogLog.cardinality();
            return new Tuple2<>(t._1, count);
          }
        });
    result.saveAsTextFile(outputDir + broadcastJob.value().getJobId());
    LOGGER.info("Output files are in directory {}", outputDir + broadcastJob.value().getJobId());
  }
}
