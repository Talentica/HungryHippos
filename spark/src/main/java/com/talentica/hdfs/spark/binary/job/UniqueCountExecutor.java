package com.talentica.hdfs.spark.binary.job;

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;

import scala.Tuple2;

public class UniqueCountExecutor extends JobExecutor{
  private static final long serialVersionUID = -5427533862413369050L;

  public UniqueCountExecutor(String[] args) {
    super(args);
  }

  @Override
  public void startJob(JavaSparkContext context, JavaRDD<byte[]> rdd,
      DataDescriptionConfig dataDescriptionConfig, Broadcast<Job> broadcastJob) {
    Broadcast<FieldTypeArrayDataDescription> dataDes =
        context.broadcast(dataDescriptionConfig.getDataDescription());
    JavaPairRDD<String, Integer> pairRDD = rdd.repartition(2000).mapToPair(new PairFunction<byte[], String, Integer>() {
      private static final long serialVersionUID = -1533590342050196085L;

      @Override
      public Tuple2<String, Integer> call(byte[] buf) throws Exception {
        HHRDDRowReader readerVar = new HHRDDRowReader(dataDes.getValue());
        ByteBuffer byteBuffer = ByteBuffer.wrap(buf);
        readerVar.setByteBuffer(byteBuffer);
        String key = "";
        for (int index = 0; index < broadcastJob.value().getDimensions().length; index++) {
          key = key + ((MutableCharArrayString) readerVar
              .readAtColumn(broadcastJob.value().getDimensions()[index])).toString();
        }
        key = key + "|id=" + broadcastJob.value().getJobId();
        Integer value = (Integer) readerVar.readAtColumn(broadcastJob.value().getCalculationIndex());
        return new Tuple2<>(key, value);
      }
    });
    JavaPairRDD<String, Iterable<Integer>> pairRDDGroupedByKey = pairRDD.groupByKey();
    JavaPairRDD<String, Long> result = pairRDDGroupedByKey
        .mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Long>() {
          private static final long serialVersionUID = 484111559311975643L;

          @Override
          public Tuple2<String, Long> call(Tuple2<String, Iterable<Integer>> t) throws Exception {
            HyperLogLog hyperLogLog = new HyperLogLog(0.01);
            Iterator<Integer> itr = t._2.iterator();
            while (itr.hasNext()) {
              hyperLogLog.offer(itr.next());
            }
            Long count = hyperLogLog.cardinality();
            return new Tuple2<>(t._1, count);
          }
        });
    result.saveAsTextFile(outputDir + broadcastJob.value().getJobId());
    LOGGER.info("Output files are in directory {}", outputDir + broadcastJob.value().getJobId());
  }
  
}
