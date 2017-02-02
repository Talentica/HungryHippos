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

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;

import scala.Tuple2;

/**
 * @author pooshans
 */
public class SumJobExecutor {

    public static JavaRDD<Tuple2<String, Long>> process(HHRDD hipposRDD, Broadcast<FieldTypeArrayDataDescription> descriptionBroadcast,
                                                        Broadcast<Job> jobBroadcast) {

        JavaPairRDD<String, Integer> javaRDD =
                hipposRDD.toJavaRDD().mapToPair(new PairFunction<byte[], String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(byte[] bytes) throws Exception {
                        HHRDDRowReader reader = new HHRDDRowReader(descriptionBroadcast.getValue());
                        reader.wrap(bytes);
                        String key = "";
                        for (int index = 0; index < jobBroadcast.value().getDimensions().length; index++) {
                            key = key + ((MutableCharArrayString) reader
                                    .readAtColumn(jobBroadcast.value().getDimensions()[index])).toString();
                        }
                        key = key + "|id=" + jobBroadcast.value().getJobId();
                        Integer value = (Integer) reader.readAtColumn(jobBroadcast.value().getCalculationIndex());
                        return new Tuple2<String, Integer>(key, value);
                    }
                });
        JavaRDD<Tuple2<String, Long>> resultRDD = javaRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Integer>>, Tuple2<String, Long>>() {

            @Override
            public Iterator<Tuple2<String, Long>> call(Iterator<Tuple2<String, Integer>> t) throws Exception {

                List<Tuple2<String, Long>> sumList = new ArrayList<>();
                Map<String, Long> map = new HashMap<>();
                while (t.hasNext()) {
                    Tuple2<String, Integer> tuple2 = t.next();
                    Long storedValue = map.get(tuple2._1);
                    if (storedValue == null) {
                        map.put(tuple2._1,tuple2._2.longValue());
                    } else {
                        map.put(tuple2._1, tuple2._2 + storedValue);
                    }
                }

                for (Map.Entry<String, Long> entry : map.entrySet()) {
                    sumList.add(new Tuple2<String, Long>(entry.getKey(), entry.getValue()));
                }
                return sumList.iterator();
            }
        }, true);

       return resultRDD;
    }

}
