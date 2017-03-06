/**
 * 
 */
package com.talentica.spark.job.executor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.reader.HHBinaryRowReader;

import scala.Tuple2;

/**
 * @author pooshans
 *
 */
public class SumExecutor<T> implements Serializable {

  private static final long serialVersionUID = 2938355892415268963L;

  public <T> JavaRDD<Tuple2<String, Long>> process(HHRDD<T> hipposRDD,
      Broadcast<FieldTypeArrayDataDescription> descriptionBroadcast, Broadcast<Job> jobBroadcast) {

    Function<Integer, Long> createCombiner = new Function<Integer, Long>() {
      @Override
      public Long call(Integer v1) throws Exception {
        return Long.valueOf(v1.intValue());
      }
    };

    Function2<Long, Integer, Long> mergeValue = new Function2<Long, Integer, Long>() {
      @Override
      public Long call(Long v1, Integer v2) throws Exception {
        Long sum = v1 + v2;
        return sum;
      }

    };

    Function2<Long, Long, Long> mergeCombiners = new Function2<Long, Long, Long>() {
      @Override
      public Long call(Long v1, Long v2) throws Exception {
        return v1 + v2;
      }
    };

    JavaPairRDD<String, Long> javaRDD =
        hipposRDD.toJavaRDD().mapToPair(new PairFunction<T, String, Integer>() {
          @Override
          public Tuple2<String, Integer> call(T type) throws Exception {
            if (type instanceof byte[]) {
              byte[] bytes = (byte[]) type;
              HHBinaryRowReader reader = new HHBinaryRowReader(descriptionBroadcast.getValue());
              reader.wrap(bytes);
              String key = "";
              for (int index = 0; index < jobBroadcast.value().getDimensions().length; index++) {
                key = key + reader.readAtColumn(jobBroadcast.value().getDimensions()[index]);
              }
              key = key + "|id=" + jobBroadcast.value().getJobId();
              Integer value =
                  (Integer) reader.readAtColumn(jobBroadcast.value().getCalculationIndex());
              return new Tuple2<String, Integer>(key, value);
            } else if (type instanceof String) {
              String line = (String) type;
              String[] token = line.split(",");
              String key = "";
              for (int index = 0; index < jobBroadcast.value().getDimensions().length; index++) {
                key = key + token[index];
              }
              key = key + "|id=" + jobBroadcast.value().getJobId();
              Integer value = Integer.valueOf(token[jobBroadcast.value().getCalculationIndex()]);
              return new Tuple2<String, Integer>(key, value);
            } else {
              throw new RuntimeException(
                  "Invalid type paramter exeception. Only supported type is byte[] and String");
            }
          }
        }).combineByKey(createCombiner, mergeValue, mergeCombiners)
            .reduceByKey(new Function2<Long, Long, Long>() {
              private static final long serialVersionUID = 5677451009262753978L;

              @Override
              public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
              }
            });;
    JavaRDD<Tuple2<String, Long>> resultRDD = javaRDD
        .mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Long>>, Tuple2<String, Long>>() {

          @Override
          public Iterator<Tuple2<String, Long>> call(Iterator<Tuple2<String, Long>> t)
              throws Exception {

            List<Tuple2<String, Long>> sumList = new ArrayList<>();
            Map<String, Long> map = new HashMap<>();
            while (t.hasNext()) {
              Tuple2<String, Long> tuple2 = t.next();
              Long storedValue = map.get(tuple2._1);
              if (storedValue == null) {
                map.put(tuple2._1, tuple2._2.longValue());
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
