package com.talentica.spark.job.executor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
import com.talentica.hungryhippos.ds.DescriptiveStatisticsNumber;

import scala.Tuple2;

/**
 * Created by rajkishoreh on 16/12/16.
 */
public class MedianJobExecutor {


  public static JavaRDD<Tuple2<String, Double>> process(HHRDD hipposRDD,
      Broadcast<FieldTypeArrayDataDescription> descriptionBroadcast, Broadcast<Job> jobBroadcast) {
    JavaPairRDD<String, Integer> pairRDD =
        hipposRDD.toJavaRDD().mapToPair(new PairFunction<byte[], String, Integer>() {
          @Override
          public Tuple2<String, Integer> call(byte[] bytes) throws Exception {
            HHRDDRowReader reader = new HHRDDRowReader(descriptionBroadcast.getValue());
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            reader.setByteBuffer(byteBuffer);
            String key = "";
            for (int index = 0; index < jobBroadcast.value().getDimensions().length; index++) {
              key = key + ((MutableCharArrayString) reader
                  .readAtColumn(jobBroadcast.value().getDimensions()[index])).toString();
            }
            key = key + "|id=" + jobBroadcast.value().getJobId();
            Integer value =
                (Integer) reader.readAtColumn(jobBroadcast.value().getCalculationIndex());
            return new Tuple2<String, Integer>(key, value);
          }
        });

    JavaRDD<Tuple2<String, Double>> resultRDD = pairRDD.mapPartitions(
        new FlatMapFunction<Iterator<Tuple2<String, Integer>>, Tuple2<String, Double>>() {
          @Override
          public Iterator<Tuple2<String, Double>> call(Iterator<Tuple2<String, Integer>> t)
              throws Exception {

            List<Tuple2<String, Double>> medianList = new ArrayList<>();
            Map<String, DescriptiveStatisticsNumber<Integer>> map = new HashMap<>();
            while (t.hasNext()) {
              Tuple2<String, Integer> tuple2 = t.next();
              DescriptiveStatisticsNumber<Integer> medianCalculator = map.get(tuple2._1);
              if (medianCalculator == null) {
                medianCalculator = new DescriptiveStatisticsNumber<Integer>();
                map.put(tuple2._1, medianCalculator);
              }
              medianCalculator.add(tuple2._2);
            }

            for (Entry<String, DescriptiveStatisticsNumber<Integer>> entry : map.entrySet()) {
              Double median = (double) entry.getValue().percentile(50);
              medianList.add(new Tuple2<String, Double>(entry.getKey(), median));
            }
            return medianList.iterator();
          }
        }, true);

    return resultRDD;
  }

}
