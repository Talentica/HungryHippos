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
package com.talentica.spark.job.executor;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.rdd.main.job.Job;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * @author sudarshans
 */
public class UniqueCountJobExecutor {

  public static JavaRDD<Tuple2<String, Long>> process(JavaRDD<byte[]> hipposRDD,
      Broadcast<DataDescription> descriptionBroadcast, Broadcast<Job> jobBroadcast) {
    JavaPairRDD<String, Integer> javaRDD =
        hipposRDD.mapToPair(new PairFunction<byte[], String, Integer>() {
          private static final long serialVersionUID = -1533590342050196085L;

                    @Override
                    public Tuple2<String, Integer> call(byte[] buf) throws Exception {
                        ByteBuffer byteBuffer = ByteBuffer.wrap(buf);
                        HHRDDRowReader readerVar = new HHRDDRowReader(descriptionBroadcast.getValue(),byteBuffer);
                        StringBuilder key = new StringBuilder();
                        for (int index = 0; index < jobBroadcast.value().getDimensions().length; index++) {
                            key.append(( readerVar
                                    .readAtColumn(jobBroadcast.value().getDimensions()[index])).toString());
                        }
                        key.append("|id=").append(jobBroadcast.value().getJobId());
                        Integer value = Integer.valueOf(readerVar.readAtColumn(jobBroadcast.value().getCalculationIndex()).toString());
                        return new Tuple2<String, Integer>(key.toString(), value);
                    }
                });
        JavaRDD<Tuple2<String, Long>> resultRDD = javaRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Integer>>, Tuple2<String, Long>>() {

          @Override
          public Iterator<Tuple2<String, Long>> call(Iterator<Tuple2<String, Integer>> t)
              throws Exception {

            List<Tuple2<String, Long>> uniqueValues = new ArrayList<>();
            Map<String, HyperLogLog> hyperLogLogMap = new HashMap<>();
            while (t.hasNext()) {
              Tuple2<String, Integer> tuple2 = t.next();
              HyperLogLog hyperLogLog = hyperLogLogMap.get(tuple2._1);
              if (hyperLogLog == null) {
                hyperLogLog = new HyperLogLog(0.01);
                hyperLogLogMap.put(tuple2._1, hyperLogLog);
              }
              hyperLogLog.offer(tuple2._2);
            }
            for (Map.Entry<String, HyperLogLog> entry : hyperLogLogMap.entrySet()) {
              uniqueValues.add(new Tuple2<>(entry.getKey(), entry.getValue().cardinality()));
            }
            return uniqueValues.iterator();
          }
        }, true);

    return resultRDD;
  }

}
