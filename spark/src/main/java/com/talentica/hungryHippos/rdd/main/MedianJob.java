package com.talentica.hungryHippos.rdd.main;

import java.io.FileNotFoundException;

import javax.xml.bind.JAXBException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import com.talentica.hungryHippos.rdd.HHRDDConfiguration;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.job.JobMatrix;
import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;

/**
 * Created by rajkishoreh on 16/12/16.
 */
public class MedianJob {

    private static JavaSparkContext context;
    private static MedianJobExecutor executor;

    public static void main(String[] args) throws FileNotFoundException, JAXBException {
        executor = new MedianJobExecutor(args);
        HHRDDConfiguration hhrddConfiguration = new HHRDDConfiguration(executor.getDistrDir(),
                executor.getClientConf(), executor.getOutputFile());
        initializeSparkContext();
        for (Job job : getSumJobMatrix().getJobs()) {
            Broadcast<Job> jobBroadcast = context.broadcast(job);
            int jobPrimDim = HHRDDHelper
                    .getPrimaryDimensionIndexToRunJobWith(job, hhrddConfiguration.getShardingIndexes());
            executor.startJob(context, hhrddConfiguration, jobBroadcast,jobPrimDim);
        }
        executor.stop(context);
    }


    private static JobMatrix getSumJobMatrix() {
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
