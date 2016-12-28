package com.talentica.hungryHippos.rdd.main;

import java.io.FileNotFoundException;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import com.talentica.hdfs.spark.binary.job.JobMatrixInterface;
import com.talentica.hungryHippos.rdd.HHRDDConfiguration;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;
import com.talentica.spark.job.executor.MedianJobExecutor;

/**
 * Created by rajkishoreh on 16/12/16.
 */
public class MedianJob {

    private static JavaSparkContext context;
    private static MedianJobExecutor executor;

    public static void main(String[] args) throws FileNotFoundException, JAXBException, ClassNotFoundException, InstantiationException, IllegalAccessException {
      getSumJobMatrix();
        executor = new MedianJobExecutor(args);
        
        HHRDDConfiguration hhrddConfiguration = new HHRDDConfiguration(executor.getDistrDir(),
                executor.getClientConf(), executor.getOutputFile());
        initializeSparkContext();
        for (Job job : getSumJobMatrix()) {
            Broadcast<Job> jobBroadcast = context.broadcast(job);
            int jobPrimDim = HHRDDHelper
                    .getPrimaryDimensionIndexToRunJobWith(job, hhrddConfiguration.getShardingIndexes());
            executor.startMedianJob(context, hhrddConfiguration, jobBroadcast,jobPrimDim);
        }
        executor.stop(context);
    }


    private static List<Job> getSumJobMatrix() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
      Class jobMatrix = Class.forName("com.talentica.hungryHippos.rdd.job.JobMatrix");
      JobMatrixInterface obj =  (JobMatrixInterface) jobMatrix.newInstance();
      obj.printMatrix();
      return obj.getJobs();
    }

    private static void initializeSparkContext() {
        if (MedianJob.context == null) {
            SparkConf conf =
                    new SparkConf().setMaster(executor.getMasterIp()).setAppName(executor.getAppName());
            try {
                MedianJob.context = new JavaSparkContext(conf);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
