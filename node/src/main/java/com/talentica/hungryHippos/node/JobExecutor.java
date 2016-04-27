/**
 * 
 */
package com.talentica.hungryHippos.node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.common.JobRunner;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZKUtils;
import com.talentica.hungryHippos.coordination.domain.LeafBean;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.Property;
import com.talentica.hungryHippos.coordination.utility.Property.PROPERTIES_NAMESPACE;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryHippos.utility.JobEntity;
import com.talentica.hungryHippos.utility.PathUtil;

/**
 * NodeStarter will accept the sharded data and do various operations i.e row
 * count per job and also execution of the aggregation of the data.
 * 
 * Created by debasishc on 1/9/15.
 */
public class JobExecutor {

	private static final Logger LOGGER = LoggerFactory.getLogger(JobExecutor.class);

	private static NodesManager nodesManager;

	private static DataStore dataStore;

	public static void main(String[] args) {
		try {
			long startTime = System.currentTimeMillis();
			String jobUUId = args[0];
			CommonUtil.loadDefaultPath(jobUUId);
			Property.initialize(PROPERTIES_NAMESPACE.NODE);
			nodesManager = Property.getNodesManagerIntances();
			waitForSignal();
			LOGGER.info("Start Node initialize");
			JobRunner jobRunner = createJobRunner();
			List<JobEntity> jobEntities = getJobsFromZKNode();
			for (JobEntity jobEntity : jobEntities) {
				Object[] loggerJobArgument = new Object[] { jobEntity.getJobId() };
				LOGGER.info("Starting execution of job: {}", loggerJobArgument);
				jobRunner.run(jobEntity);
				LOGGER.info("Finished with execution of job: {}", loggerJobArgument);
			}
			jobEntities.clear();
			String buildStartPath = ZKUtils.buildNodePath(NodeUtil.getNodeId()) + PathUtil.FORWARD_SLASH
					+ CommonUtil.ZKJobNodeEnum.FINISH_JOB_MATRIX.name();
			CountDownLatch signal = new CountDownLatch(1);
			nodesManager.createPersistentNode(buildStartPath, signal);
			signal.await();
			long endTime = System.currentTimeMillis();
			LOGGER.info("It took {} seconds of time to execute all jobs.", ((endTime - startTime) / 1000));
			LOGGER.info("ALL JOBS ARE FINISHED");
		} catch (Exception exception) {
			LOGGER.error("Error occured while executing node starter program.", exception);
		}
	}

	/**
	 * Await for the signal of the job manager. Once job is submitted, it start execution on respective servers.
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	private static void waitForSignal() throws KeeperException,
			InterruptedException {
		CountDownLatch signal = new CountDownLatch(1);
		ZKUtils.waitForSignal(JobExecutor.nodesManager.buildAlertPathByName(CommonUtil.ZKJobNodeEnum.START_JOB_MATRIX.getZKJobNode()), signal);
		signal.await();
	}

	/**
	 * Create the job runner.
	 * 
	 * @return JobRunner
	 * @throws IOException
	 */
	private static JobRunner createJobRunner() throws IOException {
		FieldTypeArrayDataDescription dataDescription = CommonUtil.getConfiguredDataDescription();
		dataDescription.setKeyOrder(Property.getShardingDimensions());
		dataStore = new FileDataStore(NodeUtil.getKeyToValueToBucketMap().size(), dataDescription, true);
		return new JobRunner(dataDescription, dataStore);
	}

	/**
	 * Get the list of jobs from ZK Node.
	 * 
	 * @return List<Job>
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	private static List<JobEntity> getJobsFromZKNode()
			throws IOException, ClassNotFoundException, InterruptedException, KeeperException {
		String buildPath = ZKUtils.buildNodePath(NodeUtil.getNodeId())
				+ PathUtil.FORWARD_SLASH
				+ CommonUtil.ZKJobNodeEnum.PUSH_JOB_NOTIFICATION.name();
		Set<LeafBean> leafs = ZKUtils.searchTree(buildPath, null, null);
		/*String jobMatrixId = leafs.iterator().next().getName();
		leafs.clear();
		String buildPathWithJobMatrixId = buildPath + PathUtil.FORWARD_SLASH + jobMatrixId;
		leafs = ZKUtils.searchTree(buildPathWithJobMatrixId, null, null);*/
		LOGGER.info("Leafs size found {}", leafs.size());
		List<JobEntity> jobEntities = new ArrayList<JobEntity>();
		for (LeafBean leaf : leafs) {
			LOGGER.info("Leaf path {} and name {}", leaf.getPath(), leaf.getName());
			String buildLeafPath = ZKUtils.getNodePath(leaf.getPath(), leaf.getName());
			LOGGER.info("Build path {}", buildLeafPath);
			LeafBean leafObject = ZKUtils.getNodeValue(buildPath, buildLeafPath, leaf.getName(), null);
			JobEntity jobEntity = (JobEntity) leafObject.getValue();
			if (jobEntity == null)
				continue;
			jobEntities.add(jobEntity);
		}
		LOGGER.info("TOTAL JOBS FOUND {}", jobEntities.size());
		return jobEntities;
	}

}
