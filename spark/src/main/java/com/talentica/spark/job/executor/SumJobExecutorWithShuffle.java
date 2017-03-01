package com.talentica.spark.job.executor;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.reader.HHBinaryRowReader;

import scala.Tuple2;

/**
 * Created by rajkishoreh on 9/1/17.
 */
public class SumJobExecutorWithShuffle {
    public static JavaPairRDD<String, Long> process(HHRDD hipposRDD, Broadcast<FieldTypeArrayDataDescription> descriptionBroadcast,
                                                        Broadcast<Job> jobBroadcast) {

            Function<Integer,Long> createCombiner = new Function<Integer,Long>(){
                @Override
                public Long call(Integer v1) throws Exception {
                    return Long.valueOf(v1.intValue());
                }
            };

            Function2<Long,Integer, Long> mergeValue = new Function2<Long,Integer, Long>(){
                @Override
                public Long call(Long v1, Integer v2) throws Exception {
                    Long sum =  v1 + v2;
                    return sum;
                }

            };

            Function2<Long,Long,Long> mergeCombiners = new Function2<Long,Long,Long>(){
                @Override
                public Long call(Long v1, Long v2) throws Exception {
                    return v1 + v2;
                }
            };

            JavaPairRDD<String, Long> pairRDD = hipposRDD.toJavaRDD().mapToPair(new PairFunction<byte[], String, Integer>() {
                private static final long serialVersionUID = -4057434571069903937L;

                @Override
                public Tuple2<String, Integer> call(byte[] buf) throws Exception {
                    HHBinaryRowReader readerVar = new HHBinaryRowReader(descriptionBroadcast.getValue());
                    readerVar.wrap(buf);
                    String key = "";
                    for (int index = 0; index < jobBroadcast.value().getDimensions().length; index++) {
                        key = key + (readerVar
                                .readAtColumn(jobBroadcast.value().getDimensions()[index])).toString();
                    }
                    key = key + "|id=" + jobBroadcast.value().getJobId();
                    Integer value = (Integer) readerVar.readAtColumn(jobBroadcast.value().getCalculationIndex());
                    return new Tuple2<String, Integer>(key, value);
                }
            }).combineByKey(createCombiner, mergeValue, mergeCombiners).reduceByKey(new Function2<Long, Long, Long>() {
                private static final long serialVersionUID = 5677451009262753978L;

                @Override
                public Long call(Long v1, Long v2) throws Exception {
                    return v1 + v2;
                }
            });

        return pairRDD;
    }
}
