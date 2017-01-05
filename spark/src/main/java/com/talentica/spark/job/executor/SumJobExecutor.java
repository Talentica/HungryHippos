/**
 *
 */
package com.talentica.spark.job.executor;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
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
 * @author pooshans
 */
public class SumJobExecutor implements Serializable {

    private static final long serialVersionUID = 4405057963355945497L;
    private static Logger LOGGER = LoggerFactory.getLogger(SumJobExecutor.class);

    @SuppressWarnings("serial")
    public void startSumJob(HHRDD hipposRDD, Broadcast<FieldTypeArrayDataDescription> descriptionBroadcast,
                            Broadcast<Job> jobBroadcast, String ouputDirectory) {

        JavaPairRDD<String, Double> javaRDD =
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
        JavaRDD<Tuple2<String, Double>> resultRDD = javaRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Double>>, Tuple2<String, Double>>() {

            @Override
            public Iterator<Tuple2<String, Double>> call(Iterator<Tuple2<String, Double>> t) throws Exception {

                List<Tuple2<String, Double>> sumList = new ArrayList<>();
                Map<String, Double> map = new HashMap<>();
                while (t.hasNext()) {
                    Tuple2<String, Double> tuple2 = t.next();
                    Double storedValue = map.get(tuple2._1);
                    if (storedValue == null) {
                        map.put(tuple2._1, tuple2._2);
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

        String outputDistributedPath = ouputDirectory + File.separator + jobBroadcast.value().getJobId();

        String outputActualPath =  HHRDDHelper.getActualPath(outputDistributedPath);
        new HHJavaRDD<Tuple2<String, Double>>(resultRDD.rdd(),
                resultRDD.classTag()).saveAsTextFile(outputActualPath);
        LOGGER.info("Output files are in directory {}", outputActualPath);
    }

    public void stop(JavaSparkContext context) {
        context.stop();
    }

}
