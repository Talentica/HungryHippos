package com.talentica.hdfs.spark.binary.job;

import java.nio.ByteBuffer;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;

import scala.Tuple2;

public class SumJobExecutor extends JobExecutor {
  private static final long serialVersionUID = -2556507041035653514L;

  public SumJobExecutor(String[] args) {
    super(args);
  }

  public void startJob(JavaSparkContext context, JavaRDD<byte[]> rdd,
      DataDescriptionConfig dataDescriptionConfig, Broadcast<Job> broadcastJob) {
    Broadcast<FieldTypeArrayDataDescription> dataDes =
        context.broadcast(dataDescriptionConfig.getDataDescription());
    
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
    
    JavaPairRDD<String, Long> pairRDD = rdd.mapToPair(new PairFunction<byte[], String, Integer>() {
      private static final long serialVersionUID = -4057434571069903937L;

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
        return new Tuple2<String, Integer>(key, value);
      }
    }).combineByKey(createCombiner, mergeValue, mergeCombiners).reduceByKey(new Function2<Long, Long, Long>() {
      private static final long serialVersionUID = 5677451009262753978L;

      @Override
      public Long call(Long v1, Long v2) throws Exception {
        return v1 + v2;
      }
    });
    pairRDD.saveAsTextFile(outputDir + broadcastJob.value().getJobId());
    LOGGER.info("Output files are in directory {}", outputDir + broadcastJob.value().getJobId());
  }
}
