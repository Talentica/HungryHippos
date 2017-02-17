/**
 * 
 */
package com.talentica.hungryHippos.rdd.main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.rdd.HHRDDInfo;
import com.talentica.hungryHippos.rdd.HHTextRDD;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.job.JobMatrix;
import com.talentica.hungryHippos.rdd.utility.HHRDDFileUtils;
import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;
import com.talentica.spark.job.executor.SumExecutor;

import scala.Tuple2;

/**
 * @author pooshans
 *
 */
public class SumJobTextFile implements Serializable {

  private static final long serialVersionUID = -1338262815432741922L;
  private static Logger LOGGER = LoggerFactory.getLogger(SumJobTextFile.class);
  private static JavaSparkContext context;

  public static void main(String[] args) throws FileNotFoundException, JAXBException,
      ClassNotFoundException, InstantiationException, IllegalAccessException {
    validateProgramArgument(args);
    String masterIp = args[0];
    String appName = args[1];
    String hhFilePath = args[2];
    String clientConfigPath = args[3];
    String outputDirectory = args[4];
    initializeSparkContext(masterIp, appName);
    HHRDDHelper.initialize(clientConfigPath);
    Map<String, HHTextRDD> cacheRDD = new HashMap<>();
    HHRDDInfo hhrddInfo = HHRDDHelper.getHhrddInfo(hhFilePath);
    Broadcast<FieldTypeArrayDataDescription> descriptionBroadcast =
        context.broadcast(hhrddInfo.getFieldDataDesc());
    for (Job job : getSumJobMatrix().getJobs()) {
      String keyOfHHRDD = HHRDDHelper.generateKeyForHHRDD(job, hhrddInfo.getShardingIndexes());
      HHTextRDD hipposRDD = cacheRDD.get(keyOfHHRDD);
      if (hipposRDD == null) {
        hipposRDD = new HHTextRDD(context, hhrddInfo, job.getDimensions(), false);
        cacheRDD.put(keyOfHHRDD, hipposRDD);
      }
      Broadcast<Job> jobBroadcast = context.broadcast(job);
      JavaRDD<Tuple2<String, Long>> resultRDD =
          new SumExecutor<String>().process(hipposRDD, descriptionBroadcast, jobBroadcast);
      String outputDistributedPath =
          outputDirectory + File.separator + jobBroadcast.value().getJobId();

      String outputActualPath = HHRDDHelper.getActualPath(outputDistributedPath);
      HHRDDFileUtils.saveAsText(resultRDD, outputActualPath);
      LOGGER.info("Output files are in directory {}", outputActualPath);
    }
    context.stop();
  }


  private static JobMatrix getSumJobMatrix()
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    JobMatrix sumJobMatrix = new JobMatrix();
    int count = 0;
    for (int i = 0; i < 3; i++) {
      sumJobMatrix.addJob(new Job(new Integer[] {i}, 6, count++));
    }
    return sumJobMatrix;
  }

  private static void initializeSparkContext(String masterIp, String appName) {
    if (SumJobTextFile.context == null) {
      SparkConf conf = new SparkConf().setMaster(masterIp).setAppName(appName);
      try {
        SumJobTextFile.context = new JavaSparkContext(conf);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
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


}
