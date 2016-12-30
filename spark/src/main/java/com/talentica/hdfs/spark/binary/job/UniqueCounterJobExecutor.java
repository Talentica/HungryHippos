package com.talentica.hdfs.spark.binary.job;

import java.io.File;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.UUID;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;

import scala.Tuple2;

public class UniqueCounterJobExecutor implements Serializable {

  private static final long serialVersionUID = 1L;

  private static Logger LOGGER = LoggerFactory.getLogger(UniqueCounterJobExecutor.class);

  private String outputDir;

  public UniqueCounterJobExecutor(String outputFileLocation) {
    this.outputDir = outputFileLocation;
  }

  public void startJob(JavaSparkContext context, JavaRDD<byte[]> rdd,
      DataDescriptionConfig dataDescriptionConfig, Broadcast<Job> broadcastJob) {
    Broadcast<FieldTypeArrayDataDescription> dataDes =
        context.broadcast(dataDescriptionConfig.getDataDescription());
    JavaPairRDD<String, String> pairRDD = rdd.mapToPair(new PairFunction<byte[], String, String>() {
      private static final long serialVersionUID = -1533590342050196085L;

      @Override
      public Tuple2<String, String> call(byte[] buf) throws Exception {
        HHRDDRowReader readerVar = new HHRDDRowReader(dataDes.getValue());
        ByteBuffer byteBuffer = ByteBuffer.wrap(buf);
        readerVar.setByteBuffer(byteBuffer);
        String key = "";
        for (int index = 0; index < broadcastJob.value().getDimensions().length; index++) {
          key = key + ((MutableCharArrayString) readerVar
              .readAtColumn(broadcastJob.value().getDimensions()[index])).toString();
        }
        key = key + "|id=" + broadcastJob.value().getJobId();
        StringBuilder builder = new StringBuilder();
        for (int index = 0; index < dataDescriptionConfig.getDataDescription()
            .getNumberOfDataFields(); index++) {
          builder.append(readerVar.readAtColumn(index).toString());
          if (index > 0) {
            builder.append(",");
          }
        }
        builder.append("\n");
        String value =
            readerVar.readAtColumn(broadcastJob.value().getCalculationIndex()).toString();
        return new Tuple2<>(key, value);
      }
    });
    JavaPairRDD<String, Iterable<String>> pairRDDGroupedByKey = pairRDD.groupByKey();
    JavaPairRDD<String, Long> result = pairRDDGroupedByKey
        .mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, Long>() {
          private static final long serialVersionUID = 484111559311975643L;

          @Override
          public Tuple2<String, Long> call(Tuple2<String, Iterable<String>> t) throws Exception {
            HyperLogLog hyperLogLog = new HyperLogLog(0.01);
            Iterator<String> itr = t._2.iterator();
            while (itr.hasNext()) {
              hyperLogLog.offer(itr.next());
            }
            Long count = hyperLogLog.cardinality();
            return new Tuple2<>(t._1, count);
          }
        });
    String jobOutputLocation = outputDir + File.separator + UUID.randomUUID();
    result.saveAsTextFile(jobOutputLocation);
    LOGGER.info("Output files are in directory {}", jobOutputLocation);
  }

}
