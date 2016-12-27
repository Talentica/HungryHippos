package com.talentica.hdfs.spark.binary.job;

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;

import scala.Tuple2;

public class MedianJobExecutor extends JobExecutor{
  private static final long serialVersionUID = -2818914937852350710L;

  public MedianJobExecutor(String[] args) {
    super(args);
  }

  @Override
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
    });
    JavaPairRDD<String, Iterable<Double>> pairRDDGroupedByKey = pairRDD.groupByKey();
    JavaPairRDD<String, Double> result = pairRDDGroupedByKey.mapToPair(new PairFunction<Tuple2<String,Iterable<Double>>, String, Double>() {
      private static final long serialVersionUID = 484111559311975643L;
      @Override
      public Tuple2<String, Double> call(Tuple2<String, Iterable<Double>> t) throws Exception {
        DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
        Iterator<Double> itr = t._2.iterator();
        while(itr.hasNext()){
          descriptiveStatistics.addValue(itr.next());
        }
        Double median = descriptiveStatistics.getPercentile(50);
        return new Tuple2<String, Double>(t._1, median);
      }});
    result.saveAsTextFile(outputDir + broadcastJob.value().getJobId());
    LOGGER.info("Output files are in directory {}",
        outputDir +  broadcastJob.value().getJobId());
  }

}
