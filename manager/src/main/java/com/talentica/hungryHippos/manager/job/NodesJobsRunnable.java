/**
 * 
 */
package com.talentica.hungryHippos.manager.job;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;

import com.talentica.hungryHippos.accumulator.Job;
import com.talentica.hungryHippos.accumulator.JobEntity;
import com.talentica.hungryHippos.utility.zookeeper.LeafBean;

/**
 * @author PooshanS
 *
 */
public interface NodesJobsRunnable {

	
	void createNodeJobService() throws IOException, InterruptedException, KeeperException, ClassNotFoundException;
	
	void scheduleTaskManager() throws InterruptedException, KeeperException, ClassNotFoundException, IOException;
	
	void addJobEntity(JobEntity jobEntity);
	
	void addJob(List<JobEntity> jobEntity);
	
	TaskManager getTaskManager();
	
	boolean sendJobRunnableNotificationToNode(JobEntity jobEntity,CountDownLatch signal) throws InterruptedException, KeeperException, ClassNotFoundException, IOException;
	
	Set<LeafBean> receiveJobSucceedNotificationFromNode() throws InterruptedException, KeeperException, ClassNotFoundException, IOException;
}
