/**
 * 
 */
package com.talentica.hungryHippos.manager.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.accumulator.Job;
import com.talentica.hungryHippos.accumulator.testJobs.TestJob;
import com.talentica.hungryHippos.manager.job.JobManager;
import com.talentica.hungryHippos.sharding.Sharding;
import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.marshaling.Reader;

/**
 * @author PooshanS
 *
 */
public class StartApp {

	/**
	 * @param args
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(StartApp.class.getName());
	private static List<Job> jobList = new ArrayList<>();
	private final static int NO_OF_NODES = Integer.valueOf(Property.getProperties().getProperty("total.nodes"));

	public static void main(String[] args) throws Exception {
		LOGGER.info("SHARDING STARTED.....");
		Sharding.doSharding(getInputReaderForSharding(), NO_OF_NODES); // do the
		LOGGER.info("SHARDING DONE!!");
		createJobMatrix();
		JobManager jobManager = new JobManager();
		jobManager.addJobList(jobList);
		jobManager.start();
	}
	
	private static void createJobMatrix(){
		int jobId = 0;
		for(int i=0;i<3;i++){
			jobList.add(new TestJob(new int[]{i}, i, 6,jobId++));
			jobList.add(new TestJob(new int[]{i}, i, 7,jobId++));
            for(int j=i+1;j<5;j++){
            	jobList.add(new TestJob(new int[]{i,j}, i, 6,jobId++));
            	jobList.add(new TestJob(new int[]{i,j}, j, 7,jobId++));
                for(int k=j+1;k<5;k++){
                	jobList.add(new TestJob(new int[]{i,j,k}, i, 6,jobId++));
                	jobList.add(new TestJob(new int[]{i,j,k}, j, 7,jobId++));
                }
            }
        }
	}

	private static Reader getInputReaderForSharding() throws IOException {
		final String inputFile = Property.getProperties().getProperty("input.file");
		com.talentica.hungryHippos.utility.marshaling.FileReader fileReader = new com.talentica.hungryHippos.utility.marshaling.FileReader(
				inputFile);
		fileReader.setNumFields(9);
		fileReader.setMaxsize(25);
		return fileReader;
	}
}
