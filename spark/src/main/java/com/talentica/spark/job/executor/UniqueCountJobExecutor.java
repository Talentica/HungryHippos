/**
 * 
 */
package com.talentica.spark.job.executor;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.rdd.CustomHHJobConfiguration;
import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.HHRDDConfigSerialized;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;
import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import javax.xml.bind.JAXBException;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * @author sudarshans
 *
 */
public class UniqueCountJobExecutor implements Serializable{

  private static final long serialVersionUID = -6660816277015211262L;
  private static Logger LOGGER = LoggerFactory.getLogger(UniqueCountJobExecutor.class);
  private String outputFile;
  private String distrDir;
  private String clientConf;
  private String masterIp;
  private String appName;
  private Map<String, HHRDD> cahceRDD;

  /**
   * @param args
   */
  public UniqueCountJobExecutor(String args[]) {
    validateProgramArgument(args);
    this.masterIp = args[0];
    this.appName = args[1];
    this.distrDir = args[2];
    this.clientConf = args[3];
    this.outputFile = args[4];
    this.cahceRDD = new HashMap<String, HHRDD>();
  }


  @SuppressWarnings("serial")
  public void startUniqueCountJob(JavaSparkContext context, CustomHHJobConfiguration customHHJobConfiguration,
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
          private static final long serialVersionUID = -1533590342050196085L;

          @Override
          public Tuple2<String, Integer> call(byte[] buf) throws Exception {
            HHRDDRowReader readerVar = new HHRDDRowReader(dataDes.getValue());
            ByteBuffer byteBuffer = ByteBuffer.wrap(buf);
            readerVar.setByteBuffer(byteBuffer);
            String key = "";
            for (int index = 0; index < jobBroadcast.value().getDimensions().length; index++) {
              key = key + ((MutableCharArrayString) readerVar
                  .readAtColumn(jobBroadcast.value().getDimensions()[index])).toString();
            }
            key = key + "|id=" + jobBroadcast.value().getJobId();
            StringBuilder builder = new StringBuilder();
            for (int index = 0; index < dataDes.getValue()
                .getNumberOfDataFields(); index++) {
              builder.append(readerVar.readAtColumn(index).toString());
              if (index > 0) {
                builder.append(",");
              }
            }
            builder.append("\n");
            Integer value = Integer.valueOf(readerVar.readAtColumn(jobBroadcast.value().getCalculationIndex()).toString());
            return new Tuple2<String, Integer>(key, value);
          }
        });
    javaRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,Integer>>, Tuple2<String, Long>>() {

      @Override
      public Iterator<Tuple2<String, Long>> call(Iterator<Tuple2<String, Integer>> t) throws Exception {

        List<Tuple2<String, Long>> uniqueValues = new ArrayList<>();
        Map<String, HyperLogLog> hyperLogLogMap = new HashMap<>();
        while(t.hasNext()){
          Tuple2<String, Integer> tuple2 = t.next();
          HyperLogLog hyperLogLog = hyperLogLogMap.get(tuple2._1);
          if(hyperLogLog==null){
            hyperLogLog = new HyperLogLog(0.01);
            hyperLogLog.offer(tuple2._2);
            hyperLogLogMap.put(tuple2._1, hyperLogLog);
          }else{
            hyperLogLog.offer(tuple2._2);
          }
        }

        for(Map.Entry<String, HyperLogLog> entry : hyperLogLogMap.entrySet()){
          uniqueValues.add(new Tuple2<String, Long>(entry.getKey(), entry.getValue().cardinality()));
        }
        return uniqueValues.iterator();
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
