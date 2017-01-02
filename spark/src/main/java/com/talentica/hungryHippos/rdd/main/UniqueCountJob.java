/**
 * 
 */
package com.talentica.hungryHippos.rdd.main;

import com.talentica.hungryHippos.rdd.CustomHHJobConfiguration;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.job.JobMatrix;
import com.talentica.spark.job.executor.UniqueCountJobExecutor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import javax.xml.bind.JAXBException;
import java.io.FileNotFoundException;
import java.io.Serializable;

/**
 * @author sudarshans
 *
 */
public class UniqueCountJob implements Serializable{

  private static final long serialVersionUID = -7856558981966923544L;
  private static JavaSparkContext context;
  private static UniqueCountJobExecutor executor;

  public static void main(String[] args) throws FileNotFoundException, JAXBException, ClassNotFoundException, InstantiationException, IllegalAccessException {
    executor = new UniqueCountJobExecutor(args);
    CustomHHJobConfiguration customHHJobConfiguration = new CustomHHJobConfiguration(executor.getDistrDir(),
            executor.getClientConf(), executor.getOutputFile());
    initializeSparkContext();
    for (Job job : getUniqueCountJobMatrix().getJobs()) {
      executor.startUniqueCountJob(context, customHHJobConfiguration, job);
    }
    executor.stop(context);
  }


  private static JobMatrix getUniqueCountJobMatrix() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    JobMatrix sumJobMatrix = new JobMatrix();
    sumJobMatrix.addJob(new Job(new Integer[] {0,1},6,0));
    return sumJobMatrix;
  }

  private static void initializeSparkContext() {
    if (UniqueCountJob.context == null) {
      SparkConf conf =
          new SparkConf().setMaster(executor.getMasterIp()).setAppName(executor.getAppName());
      try {
        UniqueCountJob.context = new JavaSparkContext(conf);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  
  

}
