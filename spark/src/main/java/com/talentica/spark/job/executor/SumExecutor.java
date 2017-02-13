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
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.reader.HHRDDBinaryRowReader;

import scala.Tuple2;

/**
 * @author pooshans
 *
 */
public class SumExecutor<T> implements Serializable {

  private static final long serialVersionUID = 2938355892415268963L;

  public <T> JavaRDD<Tuple2<String, Double>> process(HHRDD<T> hipposRDD,
      Broadcast<FieldTypeArrayDataDescription> descriptionBroadcast, Broadcast<Job> jobBroadcast) {

    JavaPairRDD<String, Double> javaRDD =
        hipposRDD.toJavaRDD().mapToPair(new PairFunction<T, String, Double>() {
          @Override
          public Tuple2<String, Double> call(T type) throws Exception {
            if (type instanceof byte[]) {
              byte[] bytes = (byte[]) type;
              HHRDDBinaryRowReader reader = new HHRDDBinaryRowReader(descriptionBroadcast.getValue());
              reader.wrap(bytes);
              String key = "";
              for (int index = 0; index < jobBroadcast.value().getDimensions().length; index++) {
                key = key + ((MutableCharArrayString) reader
                    .readAtColumn(jobBroadcast.value().getDimensions()[index])).toString();
              }
              key = key + "|id=" + jobBroadcast.value().getJobId();
              Double value =
                  (Double) reader.readAtColumn(jobBroadcast.value().getCalculationIndex());
              return new Tuple2<String, Double>(key, value);
            } else if (type instanceof String) {
              String line = (String) type;
              String[] token = line.split(",");
              String key = "";
              for (int index = 0; index < jobBroadcast.value().getDimensions().length; index++) {
                key = key + token[index];
              }
              key = key + "|id=" + jobBroadcast.value().getJobId();
              Double value = Double.valueOf(token[jobBroadcast.value().getCalculationIndex()]);
              return new Tuple2<String, Double>(key, value);
            } else {
              throw new RuntimeException(
                  "Invalid type paramter exeception. Only supported type is byte[] and String");
            }
          }
        });
    JavaRDD<Tuple2<String, Double>> resultRDD = javaRDD.mapPartitions(
        new FlatMapFunction<Iterator<Tuple2<String, Double>>, Tuple2<String, Double>>() {

          @Override
          public Iterator<Tuple2<String, Double>> call(Iterator<Tuple2<String, Double>> t)
              throws Exception {

            List<Tuple2<String, Double>> sumList = new ArrayList<>();
            Map<String, Double> map = new HashMap<>();
            while (t.hasNext()) {
              Tuple2<String, Double> tuple2 = t.next();
              Double storedValue = map.get(tuple2._1);
              if (storedValue == null) {
                map.put(tuple2._1, tuple2._2.doubleValue());
              } else {
                map.put(tuple2._1, tuple2._2 + storedValue);
              }
            }

            for (Map.Entry<String, Double> entry : map.entrySet()) {
              sumList.add(new Tuple2<String, Double>(entry.getKey(), entry.getValue()));
            }
            return sumList.iterator();
          }
        }, true);

    return resultRDD;
  }


}
