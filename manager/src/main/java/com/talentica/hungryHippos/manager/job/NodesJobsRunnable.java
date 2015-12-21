/**
 * 
 */
package com.talentica.hungryHippos.manager.job;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.KeeperException;

import com.talentica.hungryHippos.accumulator.Job;
import com.talentica.hungryHippos.utility.zookeeper.LeafBean;

/**
 * @author PooshanS
 *
 */
public interface NodesJobsRunnable {

	
	void createNodeJobExecutor() throws IOException;
	
	void scheduleJobNode() throws InterruptedException, KeeperException, ClassNotFoundException, IOException;
	
	void addJob(Job job);
	
	void addJob(List<Job> jobs);
	
	NodeJob getNodeJob();
	
	void sendJobRunnableNotificationToNode(Job job) throws InterruptedException, KeeperException;
	
	Set<LeafBean> receiveJobSucceedNotificationFromNode() throws InterruptedException, KeeperException, ClassNotFoundException, IOException;
}
