package com.talentica.hungryHippos.rdd.main;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.HHRDDConfigSerialized;
import com.talentica.hungryHippos.rdd.HHRDDConfiguration;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;

import scala.Tuple2;

/**
 * Created by rajkishoreh on 16/12/16.
 */
public class MedianJobExecutor implements Serializable {
  private static final long serialVersionUID = -8292896082222169848L;
  private static Logger LOGGER = LoggerFactory.getLogger(HHRDDExecutor.class);
  private String outputFile;
  private String distrDir;
  private String clientConf;
  private String masterIp;
  private String appName;
  private Map<Integer, HHRDD> cahceRDD;

  /**
   * @param args
   */
  public MedianJobExecutor(String args[]) {
    validateProgramArgument(args);
    this.masterIp = args[0];
    this.appName = args[1];
    this.distrDir = args[2];
    this.clientConf = args[3];
    this.outputFile = args[4];
    this.cahceRDD = new HashMap<Integer, HHRDD>();
  }


  public void startJob(JavaSparkContext context, HHRDDConfiguration hhrddConfiguration,
      Broadcast<Job> jobBroadcast, int jobPrimDim) throws FileNotFoundException, JAXBException {
    HHRDDConfigSerialized hhrddConfigSerialized =
        new HHRDDConfigSerialized(hhrddConfiguration.getRowSize(),
            hhrddConfiguration.getShardingKeyOrder(), hhrddConfiguration.getDirectoryLocation(),
            hhrddConfiguration.getShardingFolderPath(), hhrddConfiguration.getNodes(),
            hhrddConfiguration.getDataDescription(), jobPrimDim);
    Broadcast<FieldTypeArrayDataDescription> dataDes =
        context.broadcast(hhrddConfiguration.getDataDescription());
    HHRDD hipposRDD = cahceRDD.get(jobPrimDim);

    if (hipposRDD == null) {
      hipposRDD = new HHRDD(context, hhrddConfigSerialized);
      cahceRDD.put(jobPrimDim, hipposRDD);
    }
    JavaPairRDD<String, Double> pairRDD =
        hipposRDD.toJavaRDD().mapToPair(new PairFunction<byte[], String, Double>() {
          @Override
          public Tuple2<String, Double> call(byte[] bytes) throws Exception {
            HHRDDRowReader reader = new HHRDDRowReader(dataDes.getValue());
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            reader.setByteBuffer(byteBuffer);
            String key = "";
            for (int index = 0; index < jobBroadcast.value().getDimensions().length; index++) {
              key = key + ((MutableCharArrayString) reader
                  .readAtColumn(jobBroadcast.value().getDimensions()[index])).toString();
            }
            key = key + "|id=" + jobBroadcast.value().getJobId();
            Double value = (Double) reader.readAtColumn(jobBroadcast.value().getCalculationIndex());
            return new Tuple2<String, Double>(key, value);
          }
        });

    JavaPairRDD<String, Iterable<Double>> pairRDDGroupedByKey = pairRDD.groupByKey();

    JavaPairRDD<String, Double> result = pairRDDGroupedByKey
        .mapToPair(new PairFunction<Tuple2<String, Iterable<Double>>, String, Double>() {

          @Override
          public Tuple2<String, Double> call(Tuple2<String, Iterable<Double>> t) throws Exception {
            DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
            Iterator<Double> itr = t._2.iterator();
            while (itr.hasNext()) {
              descriptiveStatistics.addValue(itr.next());
            }
            Double median = descriptiveStatistics.getPercentile(50);
            return new Tuple2<String, Double>(t._1, median);
          }
        });
    result.saveAsTextFile(hhrddConfiguration.getOutputFile() + jobBroadcast.value().getJobId());
    LOGGER.info("Output files are in directory {}",
        hhrddConfiguration.getOutputFile() + jobBroadcast.value().getJobId());
  }

  public void stop(JavaSparkContext context) {
    context.stop();
  }


  public String getOutputFile() {
    return outputFile;
  }


  public void setOutputFile(String outputFile) {
    this.outputFile = outputFile;
  }


  public String getDistrDir() {
    return distrDir;
  }


  public void setDistrDir(String distrDir) {
    this.distrDir = distrDir;
  }


  public String getClientConf() {
    return clientConf;
  }


  public void setClientConf(String clientConf) {
    this.clientConf = clientConf;
  }


  public String getMasterIp() {
    return masterIp;
  }


  public void setMasterIp(String masterIp) {
    this.masterIp = masterIp;
  }


  public String getAppName() {
    return appName;
  }


  public void setAppName(String appName) {
    this.appName = appName;
  }


  private void validateProgramArgument(String args[]) {
    if (args.length < 5) {
      System.err.println(
          "Improper arguments. Please provide in  proper order. i.e <spark-master-ip> <application-name> <distributed-directory> <client-configuration> <ouput-file-name>");
      System.out.println(
          "Parameter argumes should be {spark://{master}:7077} {test-app} {/distr/data} {{client-path}/client-config.xml} {output}");
      System.exit(1);
    }
  }
}
