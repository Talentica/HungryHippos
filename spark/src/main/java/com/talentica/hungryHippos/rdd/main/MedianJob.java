package com.talentica.hungryHippos.rdd.main;

import java.io.FileNotFoundException;

import javax.xml.bind.JAXBException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import com.talentica.hungryHippos.rdd.CustomHHJobConfiguration;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.job.JobMatrix;
import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;
import com.talentica.spark.job.executor.MedianJobExecutor;

/**
 * Created by rajkishoreh on 16/12/16.
 */
public class MedianJob {

    private static JavaSparkContext context;
    private static MedianJobExecutor executor;

    public static void main(String[] args) throws FileNotFoundException, JAXBException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        executor = new MedianJobExecutor(args);
        
        CustomHHJobConfiguration customHHJobConfiguration = new CustomHHJobConfiguration(executor.getDistrDir(),
                executor.getClientConf(), executor.getOutputFile());
        initializeSparkContext();
        for (Job job : getSumJobMatrix().getJobs()) {
            executor.startMedianJob(context, customHHJobConfiguration, job);
        }
        executor.stop(context);
    }

    private static JobMatrix getSumJobMatrix() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
      JobMatrix medianJobMatrix = new JobMatrix();
      medianJobMatrix.addJob(new Job(new Integer[] {0,1},6,0));
      return medianJobMatrix;
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
