/**
 *
 */
package com.talentica.spark.job.executor;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.rdd.CustomHHJobConfiguration;
import com.talentica.hungryHippos.rdd.HHJavaRDD;
import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;
import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;
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

/**
 * @author sudarshans
 */
public class UniqueCountJobExecutor implements Serializable {

    private static final long serialVersionUID = -6660816277015211262L;
    private static Logger LOGGER = LoggerFactory.getLogger(UniqueCountJobExecutor.class);


    @SuppressWarnings("serial")
    public void startUniqueCountJob(HHRDD hipposRDD, Broadcast<FieldTypeArrayDataDescription> descriptionBroadcast,
                                    Broadcast<Job> jobBroadcast, CustomHHJobConfiguration customHHJobConfiguration) {
        JavaPairRDD<String, Integer> javaRDD =
                hipposRDD.toJavaRDD().mapToPair(new PairFunction<byte[], String, Integer>() {
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
                        StringBuilder builder = new StringBuilder();
                        for (int index = 0; index < descriptionBroadcast.getValue()
                                .getNumberOfDataFields(); index++) {
                            builder.append(readerVar.readAtColumn(index).toString());
                            if (index > 0) {
                                builder.append(",");
                            }
                        }
                        builder.append("\n");
                        Integer value = Integer.valueOf(readerVar.readAtColumn(jobBroadcast.value().getCalculationIndex()).toString());
                        return new Tuple2<String, Integer>(key, value);
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
                        hyperLogLog.offer(tuple2._2);
                        hyperLogLogMap.put(tuple2._1, hyperLogLog);
                    } else {
                        hyperLogLog.offer(tuple2._2);
                    }
                }

                for (Map.Entry<String, HyperLogLog> entry : hyperLogLogMap.entrySet()) {
                    uniqueValues.add(new Tuple2<String, Long>(entry.getKey(), entry.getValue().cardinality()));
                }
                return uniqueValues.iterator();
            }
        }, true);

        String distributedPath = customHHJobConfiguration.getOutputDirectory() + File.separator + jobBroadcast.value().getJobId();

        String actualPath =  HHRDDHelper.getActualPath(distributedPath);
        new HHJavaRDD<Tuple2<String, Long>>(resultRDD.rdd(),
                resultRDD.classTag()).saveAsTextFile(actualPath);
        LOGGER.info("Output files are in directory {}", actualPath);
    }

    public void stop(JavaSparkContext context) {
        context.stop();
    }

}
