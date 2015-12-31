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
import com.talentica.hungryHippos.accumulator.testJobs.TestJob;
import com.talentica.hungryHippos.utility.zookeeper.LeafBean;

/**
 * @author PooshanS
 *
 */
public interface NodesJobsRunnable {

	
	void createNodeJobExecutor() throws IOException, InterruptedException, KeeperException, ClassNotFoundException;
	
	void scheduleJobNode() throws InterruptedException, KeeperException, ClassNotFoundException, IOException;
	
	void addJob(Job job);
	
	void addJob(List<TestJob> jobs);
	
	NodeJob getNodeJob();
	
	boolean sendJobRunnableNotificationToNode(Job job,CountDownLatch signal) throws InterruptedException, KeeperException, ClassNotFoundException, IOException;
	
	Set<LeafBean> receiveJobSucceedNotificationFromNode() throws InterruptedException, KeeperException, ClassNotFoundException, IOException;
}
