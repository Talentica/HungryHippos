package com.talentica.hdfs.spark.text.job;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.job.JobMatrix;

import scala.Tuple2;

public class SumJob {
  
  private static Logger LOGGER = LoggerFactory.getLogger(SumJob.class);

  public static void main(String[] args){
    SparkContext sc = new SparkContext(args);
    JavaSparkContext jsc = sc.initContext();
    JavaRDD<String> rdd = jsc.textFile(sc.getDistrFile());
    for(Job job : getSumJobMatrix().getJobs()){
      Broadcast<Job> broadcastJob = jsc.broadcast(job);
      runJob(sc,rdd,broadcastJob);
    }
  }
  
  public static void runJob(SparkContext sc,JavaRDD<String> rdd,Broadcast<Job> broadcastJob){
    
    Function<Double,Double> createCombiner = new Function<Double,Double>(){
      @Override
      public Double call(Double v1) throws Exception {
        return v1;
      }
    };
    
    Function2<Double, Double, Double> mergeValue = new Function2<Double,Double,Double>(){
      @Override
      public Double call(Double v1, Double v2) throws Exception {
        return v1 + v2;
      }
      
    };
    
    Function2<Double,Double,Double> mergeCombiners = new Function2<Double,Double,Double>(){
      @Override
      public Double call(Double v1, Double v2) throws Exception {
        return v1 + v2;
      }
    };
    
    JavaPairRDD<String, Double> pairRDD = rdd.mapToPair(new PairFunction<String, String, Double>() {
      private static final long serialVersionUID = -6293440291696487370L;
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
      }}).combineByKey(createCombiner, mergeValue, mergeCombiners).reduceByKey(new Function2<Double, Double, Double>() { 
        private static final long serialVersionUID = 1684560043956707683L;
        @Override
        public Double call(Double v1, Double v2) throws Exception {
          return v1 + v2;
        }
      });
    pairRDD.saveAsTextFile(sc.getOutputDir() + broadcastJob.value().getJobId());
    LOGGER.info("Output files are in directory {}",
        sc.getOutputDir() +  broadcastJob.value().getJobId());
  }
  
  private static JobMatrix getSumJobMatrix() {
    int count = 0;
    JobMatrix sumJobMatrix = new JobMatrix();
    for (int i = 0; i < 3; i++) {
      sumJobMatrix.addJob(new Job(new Integer[] {i}, 6, count++));
      sumJobMatrix.addJob(new Job(new Integer[] {i}, 7, count++));
      for (int j = i + 1; j < 4; j++) {
        sumJobMatrix.addJob(new Job(new Integer[] {i, j}, 6, count++));
        sumJobMatrix.addJob(new Job(new Integer[] {i, j}, 7, count++));
        for (int k = j + 1; k < 4; k++) {
          sumJobMatrix.addJob(new Job(new Integer[] {i, j, k}, 6, count++));
          sumJobMatrix.addJob(new Job(new Integer[] {i, j, k}, 7, count++));
        }
      }
    }
    return sumJobMatrix;
  }
  
}
