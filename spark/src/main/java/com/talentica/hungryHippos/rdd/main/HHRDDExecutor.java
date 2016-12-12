/**
 * 
 */
package com.talentica.hungryHippos.rdd.main;

import java.io.FileNotFoundException;
import java.io.Serializable;

import javax.xml.bind.JAXBException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.HHRDDConfigSerialized;
import com.talentica.hungryHippos.rdd.HHRDDConfiguration;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;
import com.talentica.hungryHippos.rdd.utility.BroadcastVariable;

import scala.Tuple2;

/**
 * @author pooshans
 *
 */
public class HHRDDExecutor implements Serializable {

  private static final long serialVersionUID = 4405057963355945497L;
  private static Logger LOGGER = LoggerFactory.getLogger(HHRDDExecutor.class);
  private String outputFile;
  private String distrDir;
  private String clientConf;
  private String masterIp;
  private String appName;

  /**
   * @param args
   */
  public HHRDDExecutor(String args[]) {
    validateProgramArgument(args);
    this.masterIp = args[0];
    this.appName = args[1];
    this.distrDir = args[2];
    this.clientConf = args[3];
    this.outputFile = args[4];
  }


  @SuppressWarnings("serial")
  public void startSumJob(JavaSparkContext context, HHRDDConfiguration hhrddConfiguration,
      BroadcastVariable broadcastVariable) throws FileNotFoundException, JAXBException {
    HHRDDConfigSerialized hhrddConfigSerialized = new HHRDDConfigSerialized(
        hhrddConfiguration.getRowSize(), hhrddConfiguration.getShardingIndexes(),
        hhrddConfiguration.getDirectoryLocation(), hhrddConfiguration.getShardingFolderPath(),
        hhrddConfiguration.getNodes(), hhrddConfiguration.getDataDescription(),broadcastVariable);

    HHRDD hipposRDD = new HHRDD(context, hhrddConfigSerialized);
    JavaPairRDD<String, Double> javaRDD =
        hipposRDD.toJavaRDD().mapToPair(new PairFunction<HHRDDRowReader, String, Double>() {
          @Override
          public Tuple2<String, Double> call(HHRDDRowReader reader) throws Exception {
            String key = "";
            for (int index =
                0; index < broadcastVariable.getJob().value().getDimensions().length; index++) {
              key = key + ((MutableCharArrayString) reader
                  .readAtColumn(broadcastVariable.getJob().value().getDimensions()[index]))
                      .toString();
            }
            key = key + "|id=" + broadcastVariable.getJob().value().getJobId();
            Double value = (Double) reader
                .readAtColumn(broadcastVariable.getJob().value().getCalculationIndex());
            return new Tuple2<String, Double>(key, value);
          }
        }).reduceByKey(new Function2<Double, Double, Double>() {
          public Double call(Double x, Double y) {
            return x + y;
          }
        });
    javaRDD.saveAsTextFile(
        hhrddConfiguration.getOutputFile() + broadcastVariable.getJob().value().getJobId());
    LOGGER.info("Output files are in directory {}",
        hhrddConfiguration.getOutputFile() + broadcastVariable.getJob().value().getJobId());
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
