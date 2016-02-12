/**
 * 
 */
package com.talentica.hungryHippos.master.job;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;

import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.common.TaskEntity;
import com.talentica.hungryHippos.coordination.domain.LeafBean;

/**
 * @author PooshanS
 *
 */
public interface NodesJobsRunnable {

	
	void createNodeJobService() throws IOException, InterruptedException, KeeperException, ClassNotFoundException;
	
	void scheduleTaskManager() throws InterruptedException, KeeperException, ClassNotFoundException, IOException;
	
	public void addJob(Job job);
	
	void addJobs(List<Job> jobs);
	
	TaskManager getTaskManager();
	
	boolean sendJobRunnableNotificationToNode(Job jobEntity,CountDownLatch signal) throws InterruptedException, KeeperException, ClassNotFoundException, IOException;
	
	Set<LeafBean> receiveJobSucceedNotificationFromNode() throws InterruptedException, KeeperException, ClassNotFoundException, IOException;
}
