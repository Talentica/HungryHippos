package com.talentica.hdfs.spark.binary.job;

import java.nio.ByteBuffer;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;

import scala.Tuple2;

public class SumJobExecutor extends JobExecutor{
  private static final long serialVersionUID = -2556507041035653514L;

  public SumJobExecutor(String[] args){
    super(args);
  }
  
  public void startJob(JavaSparkContext context, JavaRDD<byte[]> rdd,
      DataDescriptionConfig dataDescriptionConfig, Broadcast<Job> broadcastJob) {
    Broadcast<FieldTypeArrayDataDescription> dataDes =
        context.broadcast(dataDescriptionConfig.getDataDescription());
    JavaPairRDD<String, Double> pairRDD = rdd.mapToPair(new PairFunction<byte[], String, Double>() {
      private static final long serialVersionUID = -4057434571069903937L;

      @Override
      public Tuple2<String, Double> call(byte[] buf) throws Exception {
        HHRDDRowReader readerVar = new HHRDDRowReader(dataDes.getValue());
        ByteBuffer byteBuffer = ByteBuffer.wrap(buf);
        readerVar.setByteBuffer(byteBuffer);
        String key = "";
        for (int index = 0; index < broadcastJob.value().getDimensions().length; index++) {
          key = key + ((MutableCharArrayString) readerVar
              .readAtColumn(broadcastJob.value().getDimensions()[index])).toString();
        }
        key = key + "|id=" + broadcastJob.value().getJobId();
        Double value = (Double) readerVar.readAtColumn(broadcastJob.value().getCalculationIndex());
        return new Tuple2<String, Double>(key, value);
      }
    }).reduceByKey(new Function2<Double, Double, Double>() {
      private static final long serialVersionUID = 5677451009262753978L;

      @Override
      public Double call(Double v1, Double v2) throws Exception {
        return v1 + v2;
      }
    });
    pairRDD.saveAsTextFile(outputDir + broadcastJob.value().getJobId());
    LOGGER.info("Output files are in directory {}", outputDir + broadcastJob.value().getJobId());
  }
}
