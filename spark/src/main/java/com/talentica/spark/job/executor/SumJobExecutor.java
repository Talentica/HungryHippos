/**
 * 
 */
package com.talentica.spark.job.executor;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;

import javax.xml.bind.JAXBException;

import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.HHRDDConfigSerialized;
import com.talentica.hungryHippos.rdd.CustomHHJobConfiguration;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;

import scala.Tuple2;

/**
 * @author pooshans
 *
 */
public class SumJobExecutor implements Serializable {

  private static final long serialVersionUID = 4405057963355945497L;
  private static Logger LOGGER = LoggerFactory.getLogger(SumJobExecutor.class);
  private String outputFile;
  private String distrDir;
  private String clientConf;
  private String masterIp;
  private String appName;
  private Map<String, HHRDD> cahceRDD;

  /**
   * @param args
   */
  public SumJobExecutor(String args[]) {
    validateProgramArgument(args);
    this.masterIp = args[0];
    this.appName = args[1];
    this.distrDir = args[2];
    this.clientConf = args[3];
    this.outputFile = args[4];
    this.cahceRDD = new HashMap<String, HHRDD>();
  }


  @SuppressWarnings("serial")
  public void startSumJob(JavaSparkContext context, CustomHHJobConfiguration customHHJobConfiguration,
      Job job) throws FileNotFoundException, JAXBException {
    HHRDDConfigSerialized hhrddConfigSerialized =
            HHRDDHelper.getHhrddConfigSerialized(customHHJobConfiguration.getDistributedPath(),
                    customHHJobConfiguration.getClientConfigPat());
    Broadcast<FieldTypeArrayDataDescription> dataDes =
        context.broadcast(hhrddConfigSerialized.getFieldTypeArrayDataDescription());
    Broadcast<Job> jobBroadcast = context.broadcast(job);
    String keyOfHHRDD = HHRDDHelper
            .generateKeyForHHRDD(job, hhrddConfigSerialized.getShardingIndexes());
    HHRDD hipposRDD = cahceRDD.get(keyOfHHRDD);
    
    if (hipposRDD == null) {
      hipposRDD = new HHRDD(context, hhrddConfigSerialized,job.getDimensions());
      cahceRDD.put(keyOfHHRDD, hipposRDD);
    }
    JavaPairRDD<String, Integer> javaRDD =
        hipposRDD.toJavaRDD().mapToPair(new PairFunction<byte[], String, Integer>() {
          @Override
          public Tuple2<String, Integer> call(byte[] bytes) throws Exception {
            HHRDDRowReader reader = new HHRDDRowReader(dataDes.getValue());
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            reader.setByteBuffer(byteBuffer);
            String key = "";
            for (int index = 0; index < jobBroadcast.value().getDimensions().length; index++) {
              key = key + ((MutableCharArrayString) reader
                  .readAtColumn(jobBroadcast.value().getDimensions()[index])).toString();
            }
            key = key + "|id=" + jobBroadcast.value().getJobId();
            Integer value = (Integer) reader.readAtColumn(jobBroadcast.value().getCalculationIndex());
            return new Tuple2<String, Integer>(key, value);
          }
        });
    javaRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,Integer>>, Tuple2<String, Long>>() {

      @Override
      public Iterator<Tuple2<String, Long>> call(Iterator<Tuple2<String, Integer>> t) throws Exception {

        List<Tuple2<String, Long>> sumList = new ArrayList<>();
        Map<String, Long> map = new HashMap<>();
        while(t.hasNext()){
          Tuple2<String, Integer> tuple2 = t.next();
          Long storedValue = map.get(tuple2._1);
          if(storedValue==null){
            map.put(tuple2._1, Long.valueOf(tuple2._2.intValue()));
          }else{
            map.put(tuple2._1, tuple2._2+storedValue);
          }
        }

        for(Map.Entry<String, Long> entry : map.entrySet()){
          sumList.add(new Tuple2<String, Long>(entry.getKey(), entry.getValue()));
        }
        return sumList.iterator();
      }
    }, true).saveAsTextFile(customHHJobConfiguration.getOutputFileName() + jobBroadcast.value().getJobId());
    LOGGER.info("Output files are in directory {}",
        customHHJobConfiguration.getOutputFileName() + jobBroadcast.value().getJobId());

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
