package com.talentica.hdfs.spark.text.job;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hdfs.spark.binary.job.JobMatrixInterface;
import com.talentica.hungryHippos.rdd.job.Job;

import scala.Tuple2;

public class SumJob {
  
  private static Logger LOGGER = LoggerFactory.getLogger(SumJob.class);

  public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException{
    SparkContext sc = new SparkContext(args);
    JavaSparkContext jsc = sc.initContext();
    JavaRDD<String> rdd = jsc.textFile(sc.getDistrFile());
    for(Job job : getSumJobMatrix()){
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
  
  private static List<Job> getSumJobMatrix() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    /*JobMatrix medianJobMatrix = new JobMatrix();
    medianJobMatrix.addJob(new Job(new Integer[] {0,1},6,0));
    return medianJobMatrix;*/
    Class jobMatrix = Class.forName("com.talentica.hungryHippos.rdd.job.JobMatrix");
    JobMatrixInterface obj =  (JobMatrixInterface) jobMatrix.newInstance();
    obj.printMatrix();
    return obj.getJobs();
  }
  
}
