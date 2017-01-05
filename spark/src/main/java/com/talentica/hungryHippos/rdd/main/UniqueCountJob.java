/**
 * 
 */
package com.talentica.hungryHippos.rdd.main;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.HHRDDConfigSerialized;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.job.JobMatrix;
import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;
import com.talentica.spark.job.executor.UniqueCountJobExecutor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import javax.xml.bind.JAXBException;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author sudarshans
 *
 */
public class UniqueCountJob implements Serializable{

  private static final long serialVersionUID = -7856558981966923544L;
  private static JavaSparkContext context;

  public static void main(String[] args) throws FileNotFoundException, JAXBException, ClassNotFoundException, InstantiationException, IllegalAccessException {
    validateProgramArgument(args);
    String masterIp = args[0];
    String appName = args[1];
    String distrDir = args[2];
    String clientConfigPath = args[3];
    String outputDirectory = args[4];
    initializeSparkContext(masterIp,appName);
    HHRDDHelper.initialize(clientConfigPath);

    UniqueCountJobExecutor executor = new UniqueCountJobExecutor();
    Map<String,HHRDD> cacheRDD = new HashMap<>();
    HHRDDConfigSerialized hhrddConfigSerialized = HHRDDHelper.getHhrddConfigSerialized(distrDir);
    Broadcast<FieldTypeArrayDataDescription> descriptionBroadcast =
            context.broadcast(hhrddConfigSerialized.getFieldTypeArrayDataDescription());
    for (Job job : getUniqueCountJobMatrix().getJobs()) {
      String keyOfHHRDD = HHRDDHelper.generateKeyForHHRDD(job, hhrddConfigSerialized.getShardingIndexes());
      HHRDD hipposRDD = cacheRDD.get(keyOfHHRDD);
      if (hipposRDD == null) {
        hipposRDD = new HHRDD(context, hhrddConfigSerialized,job.getDimensions());
        cacheRDD.put(keyOfHHRDD, hipposRDD);
      }
      Broadcast<Job> jobBroadcast = context.broadcast(job);
      executor.startUniqueCountJob(hipposRDD,descriptionBroadcast,jobBroadcast, outputDirectory);
    }
    executor.stop(context);
  }


  private static JobMatrix getUniqueCountJobMatrix() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    JobMatrix sumJobMatrix = new JobMatrix();
    sumJobMatrix.addJob(new Job(new Integer[] {0,1},6,0));
    return sumJobMatrix;
  }

  private static void validateProgramArgument(String args[]) {
    if (args.length < 5) {
      System.err.println(
              "Improper arguments. Please provide in  proper order. i.e <spark-master-ip> <application-name> <distributed-directory> <client-configuration> <ouput-file-name>");
      System.out.println(
              "Parameter argumes should be {spark://{master}:7077} {test-app} {/distr/data} {{client-path}/client-config.xml} {output}");
      System.exit(1);
    }
  }

  private static void initializeSparkContext(String masterIp, String appName) {
    if (context == null) {
      SparkConf conf =
              new SparkConf().setMaster(masterIp).setAppName(appName);
      try {
        context = new JavaSparkContext(conf);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

}
