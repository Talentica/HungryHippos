package com.talentica.hdfs.spark.text.job;

import java.util.Iterator;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.job.JobMatrix;

import scala.Tuple2;

public class MedianJob {
  
  private static Logger LOGGER = LoggerFactory.getLogger(MedianJob.class);
  
  public static void main(String[] args){
    SparkContext sc = new SparkContext(args);
    JavaSparkContext jsc = sc.initContext();
    JavaRDD<String> rdd = jsc.textFile(sc.getDistrFile());
    for(Job job : getMedianJobMatrix().getJobs()){
      Broadcast<Job> broadcastJob = jsc.broadcast(job);
      runJob(sc,rdd,broadcastJob);
    }
  }
  
  public static void runJob(SparkContext sc,JavaRDD<String> rdd,Broadcast<Job> broadcastJob){
    JavaPairRDD<String, Double> pairRDD = rdd.mapToPair(new PairFunction<String, String, Double>() {
      private static final long serialVersionUID = -1129787304947692082L;
      @Override
      public Tuple2<String, Double> call(String t) throws Exception {
        String[] line = t.split(",");
        String key = "";
        for (int index = 0; index < broadcastJob.value().getDimensions().length; index++) {
          key =
              key + line[broadcastJob.value().getDimensions()[index]];          
        }
        key = key + "|id=" +  broadcastJob.value().getJobId();
        Double value = new Double(line[broadcastJob.value().getCalculationIndex()]);
        return new Tuple2<String, Double>(key,value);
      }});
    JavaPairRDD<String, Iterable<Double>> pairRDDGroupedByKey = pairRDD.groupByKey();
    JavaPairRDD<String, Double> result = pairRDDGroupedByKey.mapToPair(new PairFunction<Tuple2<String,Iterable<Double>>, String, Double>() {
      private static final long serialVersionUID = 484111559311975643L;
      @Override
      public Tuple2<String, Double> call(Tuple2<String, Iterable<Double>> t) throws Exception {
        DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
        Iterator<Double> itr = t._2.iterator();
        while(itr.hasNext()){
          descriptiveStatistics.addValue(itr.next().doubleValue());
        }
        Double median = descriptiveStatistics.getPercentile(50);
        return new Tuple2<String, Double>(t._1, median);
      }});
    result.saveAsTextFile(sc.getOutputDir() + broadcastJob.value().getJobId());
    LOGGER.info("Output files are in directory {}",
        sc.getOutputDir() +  broadcastJob.value().getJobId());
  }
  
  private static JobMatrix getMedianJobMatrix() {
    int count = 0;
    JobMatrix medianJobMatrix = new JobMatrix();
    for (int i = 0; i < 1; i++) {
      medianJobMatrix.addJob(new Job(new Integer[] {i}, 6, count++));
    }
    return medianJobMatrix;
  }
}
