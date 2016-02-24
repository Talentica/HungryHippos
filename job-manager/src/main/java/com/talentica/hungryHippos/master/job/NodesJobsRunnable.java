/**
 * 
 */
package com.talentica.hungryHippos.master.job;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;

import com.talentica.hungryHippos.coordination.domain.LeafBean;
import com.talentica.hungryHippos.utility.JobEntity;

/**
 * @author PooshanS
 *
 */
public interface NodesJobsRunnable {

	
	void createNodeJobService() throws IOException, InterruptedException, KeeperException, ClassNotFoundException;
	
	void scheduleTaskManager() throws InterruptedException, KeeperException, ClassNotFoundException, IOException;
	
	public void addJob(JobEntity jobEntity);
	
	void addJobs(List<JobEntity> jobEntities);
	
	TaskManager getTaskManager();
	
	boolean sendJobRunnableNotificationToNode(JobEntity jobEntity,CountDownLatch signal) throws InterruptedException, KeeperException, ClassNotFoundException, IOException;
	
	Set<LeafBean> receiveJobSucceedNotificationFromNode() throws InterruptedException, KeeperException, ClassNotFoundException, IOException;
}
