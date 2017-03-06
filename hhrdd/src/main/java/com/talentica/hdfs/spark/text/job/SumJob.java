package com.talentica.hdfs.spark.text.job;

import org.apache.spark.SparkConf;
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
  private static JavaSparkContext context;
  
  public static void main(String[] args){
    String masterIp = args[0];
    String appName = args[1];
    String inputFile = args[2];
    String outputDir = args[3];
    
    initSparkContext(masterIp,appName);
    
    JavaRDD<String> rdd = context.textFile(inputFile);
    for(Job job : getSumJobMatrix().getJobs()){
      Broadcast<Job> broadcastJob = context.broadcast(job);
      runJob(rdd,broadcastJob,outputDir);
    }
  }
  
  private static void initSparkContext(String masterIp,String appName){
    if(context == null){
      SparkConf conf = new SparkConf().setMaster(masterIp).setAppName(appName);
      context = new JavaSparkContext(conf);
    }
  }
  
  public static void runJob(JavaRDD<String> rdd,Broadcast<Job> broadcastJob,String outputDir){
    
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
    pairRDD.saveAsTextFile(outputDir + broadcastJob.value().getJobId());
    LOGGER.info("Output files are in directory {}",
        outputDir +  broadcastJob.value().getJobId());
  }
  
  private static JobMatrix getSumJobMatrix(){
    JobMatrix medianJobMatrix = new JobMatrix();
    medianJobMatrix.addJob(new Job(new Integer[] {0,1},6,0));
    return medianJobMatrix;
  }
  
}
