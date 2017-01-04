package com.talentica.spark.job.executor;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.rdd.CustomHHJobConfiguration;
import com.talentica.hungryHippos.rdd.HHJavaRDD;
import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;
import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;

/**
 * Created by rajkishoreh on 16/12/16.
 */
public class MedianJobExecutor implements Serializable {
    private static final long serialVersionUID = -8292896082222169848L;
    private static Logger LOGGER = LoggerFactory.getLogger(SumJobExecutor.class);


    public void startMedianJob(HHRDD hipposRDD, Broadcast<FieldTypeArrayDataDescription> descriptionBroadcast,
                               Broadcast<Job> jobBroadcast, CustomHHJobConfiguration customHHJobConfiguration) {
        JavaPairRDD<String, Double> pairRDD =
                hipposRDD.toJavaRDD().mapToPair(new PairFunction<byte[], String, Double>() {
                    @Override
                    public Tuple2<String, Double> call(byte[] bytes) throws Exception {
                        HHRDDRowReader reader = new HHRDDRowReader(descriptionBroadcast.getValue());
                        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
                        reader.setByteBuffer(byteBuffer);
                        String key = "";
                        for (int index = 0; index < jobBroadcast.value().getDimensions().length; index++) {
                            key = key + ((MutableCharArrayString) reader
                                    .readAtColumn(jobBroadcast.value().getDimensions()[index])).toString();
                        }
                        key = key + "|id=" + jobBroadcast.value().getJobId();
                        Double value = (Double) reader.readAtColumn(jobBroadcast.value().getCalculationIndex());
                        return new Tuple2<String, Double>(key, value);
                    }
                });

        JavaRDD<Tuple2<String, Double>> resultRDD = pairRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Double>>, Tuple2<String, Double>>() {
            @Override
            public Iterator<Tuple2<String, Double>> call(Iterator<Tuple2<String, Double>> t) throws Exception {

                List<Tuple2<String, Double>> medianList = new ArrayList<>();
                Map<String, DescriptiveStatistics> map = new HashMap<>();
                while (t.hasNext()) {
                    Tuple2<String, Double> tuple2 = t.next();
                    DescriptiveStatistics descriptiveStatistics = map.get(tuple2._1);
                    if (descriptiveStatistics == null) {
                        descriptiveStatistics = new DescriptiveStatistics();
                        map.put(tuple2._1, descriptiveStatistics);
                    }
                    descriptiveStatistics.addValue(tuple2._2);
                }

                for (Entry<String, DescriptiveStatistics> entry : map.entrySet()) {
                    Double median = entry.getValue().getPercentile(50);
                    medianList.add(new Tuple2<String, Double>(entry.getKey(), median));
                }
                return medianList.iterator();
            }
        }, true);

        String distributedPath = customHHJobConfiguration.getOutputDirectory() + File.separator + jobBroadcast.value().getJobId();

        String actualPath =  HHRDDHelper.getActualPath(distributedPath);
        new HHJavaRDD<Tuple2<String, Double>>(resultRDD.rdd(),
                resultRDD.classTag()).saveAsTextFile(actualPath);
        LOGGER.info("Output files are in directory {}", actualPath);
    }

    public void stop(JavaSparkContext context) {
        context.stop();
    }

}
