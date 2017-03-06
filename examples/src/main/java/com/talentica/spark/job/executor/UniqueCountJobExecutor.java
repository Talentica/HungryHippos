/**
 *
 */
package com.talentica.spark.job.executor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;

import scala.Tuple2;

/**
 * @author sudarshans
 */
public class UniqueCountJobExecutor {

  public static JavaRDD<Tuple2<String, Long>> process(JavaRDD<byte[]> hipposRDD,
      Broadcast<DataDescription> descriptionBroadcast,
                                                        Broadcast<Job> jobBroadcast) {
        JavaPairRDD<String, Integer> javaRDD =
        hipposRDD.mapToPair(new PairFunction<byte[], String, Integer>() {
                    private static final long serialVersionUID = -1533590342050196085L;

                    @Override
                    public Tuple2<String, Integer> call(byte[] buf) throws Exception {
                        HHRDDRowReader readerVar = new HHRDDRowReader(descriptionBroadcast.getValue());
                        ByteBuffer byteBuffer = ByteBuffer.wrap(buf);
                        readerVar.setByteBuffer(byteBuffer);
                        String key = "";
                        for (int index = 0; index < jobBroadcast.value().getDimensions().length; index++) {
                            key = key + ((MutableCharArrayString) readerVar
                                    .readAtColumn(jobBroadcast.value().getDimensions()[index])).toString();
                        }
                        key = key + "|id=" + jobBroadcast.value().getJobId();
                        Integer value = Integer.valueOf(readerVar.readAtColumn(jobBroadcast.value().getCalculationIndex()).toString());
                        return new Tuple2<>(key, value);
                    }
                });
        JavaRDD<Tuple2<String, Long>> resultRDD = javaRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Integer>>, Tuple2<String, Long>>() {

            @Override
            public Iterator<Tuple2<String, Long>> call(Iterator<Tuple2<String, Integer>> t) throws Exception {

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
