package com.talentica.hdfs.spark.binary.job;

import java.nio.ByteBuffer;

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

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.job.JobMatrix;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;

import scala.Tuple2;

public class SumJob {

  private static JavaSparkContext context;
  protected static Logger LOGGER = LoggerFactory.getLogger(SumJob.class);

  public static void main(String[] args) {
    String masterIp = args[0];
    String appName = args[1];
    String inputFile = args[2];
    String outputDir = args[3];
    String shardingFolderPath = args[4];

    initSparkContext(masterIp, appName);

    DataDescriptionConfig dataDescriptionConfig = new DataDescriptionConfig(shardingFolderPath);
    JavaRDD<byte[]> rdd = context.binaryRecords(inputFile, dataDescriptionConfig.getRowSize());
    Broadcast<FieldTypeArrayDataDescription> dataDes =
        context.broadcast(dataDescriptionConfig.getDataDescription());
    for (Job job : getSumJobMatrix().getJobs()) {
      Broadcast<Job> broadcastJob = context.broadcast(job);
      startJob(rdd, dataDes, broadcastJob, outputDir);
    }
    context.stop();
  }

  private static void initSparkContext(String masterIp, String appName) {
    if (context == null) {
      SparkConf conf = new SparkConf().setMaster(masterIp).setAppName(appName);
      context = new JavaSparkContext(conf);
    }
  }

  private static JobMatrix getSumJobMatrix() {
    JobMatrix sumJobMatrix = new JobMatrix();
    int count = 0;

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

  public static void startJob(JavaRDD<byte[]> rdd, Broadcast<FieldTypeArrayDataDescription> dataDes,
      Broadcast<Job> broadcastJob, String outputDir) {
    Function<Double, Double> createCombiner = new Function<Double, Double>() {
      private static final long serialVersionUID = 6547151567329751479L;

      @Override
      public Double call(Double v1) throws Exception {
        return v1;
      }
    };

    Function2<Double, Double, Double> mergeValue = new Function2<Double, Double, Double>() {
      private static final long serialVersionUID = 8342701311730308998L;

      @Override
      public Double call(Double v1, Double v2) throws Exception {
        return v1 + v2;
      }

    };

    Function2<Double, Double, Double> mergeCombiners = new Function2<Double, Double, Double>() {
      private static final long serialVersionUID = -5698647671298354646L;

      @Override
      public Double call(Double v1, Double v2) throws Exception {
        return v1 + v2;
      }
    };

    JavaPairRDD<String, Double> pairRDD = rdd.mapToPair(new PairFunction<byte[], String, Double>() {
      private static final long serialVersionUID = -4057434571069903937L;

      @Override
      public Tuple2<String, Double> call(byte[] buf) throws Exception {
        HHRDDRowReader readerVar = new HHRDDRowReader(dataDes.getValue());
        readerVar.wrap(buf);
        String key = "";
        for (int index = 0; index < broadcastJob.value().getDimensions().length; index++) {
          key = key + ((MutableCharArrayString) readerVar
              .readAtColumn(broadcastJob.value().getDimensions()[index])).toString();
        }
        key = key + "|id=" + broadcastJob.value().getJobId();
        Double value = (Double) readerVar.readAtColumn(broadcastJob.value().getCalculationIndex());
        return new Tuple2<String, Double>(key, value);
      }
    }).combineByKey(createCombiner, mergeValue, mergeCombiners)
        .reduceByKey(new Function2<Double, Double, Double>() {
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
