package com.talentica.hadoop.spark.median;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hadoop.spark.job.DataDescriptionConfig;
import com.talentica.hadoop.spark.job.JobExecutor;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;

import scala.Tuple2;

public class MedianJobExecutor implements Serializable{

  private static final long serialVersionUID = -1053747159455824957L;
  
  private static Logger LOGGER = LoggerFactory.getLogger(JobExecutor.class);
  private String outputDir;
  private String distrFile;
  private String masterIp;
  private String appName;
  private String shardingFolderPath;
  
  public MedianJobExecutor(String [] args){
    
    validateProgramArgument(args);
    this.masterIp = args[0];
    this.appName = args[1];
    this.distrFile = args[2];
    this.outputDir = args[3];
    this.shardingFolderPath = args[4];
  }
  
  public void startJob(JavaSparkContext context, DataDescriptionConfig dataDescriptionConfig,
      Broadcast<Job> broadcastJob){
    JavaRDD<byte[]> rdd = context.binaryRecords(distrFile, dataDescriptionConfig.getRowSize());
    HHRDDRowReader reader = new HHRDDRowReader(dataDescriptionConfig.getDataDescription());
    Broadcast<HHRDDRowReader> readerVar = context.broadcast(reader);
    JavaPairRDD<String, Double> pairRDD = rdd.mapToPair(new PairFunction<byte[], String, Double>() {
      private static final long serialVersionUID = -4057434571069903937L;
      @Override
      public Tuple2<String, Double> call(byte[] buf) throws Exception {
        ByteBuffer byteBuffer = ByteBuffer.wrap(buf);
        readerVar.getValue().setByteBuffer(byteBuffer);
        String key = "";
        for (int index = 0; index < broadcastJob.value().getDimensions().length; index++) {
          key =
              key + ((MutableCharArrayString)readerVar.getValue().
                  readAtColumn(broadcastJob.value().getDimensions()[index])).toString();          
        }
        key = key + "|id=" +  broadcastJob.value().getJobId();
        Double value = (Double) readerVar.getValue().readAtColumn(broadcastJob.value().getCalculationIndex());
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
          descriptiveStatistics.addValue(itr.next());
        }
        Double median = descriptiveStatistics.getPercentile(50);
        return new Tuple2<String, Double>(t._1, median);
      }});
    result.saveAsTextFile(outputDir + broadcastJob.value().getJobId());
    LOGGER.info("Output files are in directory {}",
        outputDir +  broadcastJob.value().getJobId());
  }
  
  public String getOutputDir() {
    return outputDir;
  }

  public String getDistrFile() {
    return distrFile;
  }

  public String getMasterIp() {
    return masterIp;
  }

  public String getAppName() {
    return appName;
  }

  public String getShardingFolderPath() {
    return shardingFolderPath;
  }

  private void validateProgramArgument(String[] args){
    if (args.length < 5) {
      System.err.println(
          "Improper arguments. Please provide in  proper order. i.e <spark-master-ip> <application-name> <distributed-file> <ouput-dir-name> <sharding-config-folder>");
      System.out.println(
          "Parameter argumes should be {spark://{master}:7077} {test-app} {hdfs://master-ip:port/distr/data} {output} {path/shardingConfig}");
      System.exit(1);
    }
  }
}
