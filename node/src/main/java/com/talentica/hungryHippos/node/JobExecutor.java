/**
 * 
 */
package com.talentica.hungryHippos.node;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.common.IntArrayKeyHashMap;
import com.talentica.hungryHippos.common.JobRunner;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZKUtils;
import com.talentica.hungryHippos.coordination.domain.LeafBean;
import com.talentica.hungryHippos.coordination.domain.ServerHeartBeat;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryHippos.storage.NodeDataStoreIdCalculator;
import com.talentica.hungryHippos.utility.CommonUtil;
import com.talentica.hungryHippos.utility.JobEntity;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.Property.PROPERTIES_NAMESPACE;

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

	private static NodeDataStoreIdCalculator nodeDataStoreIdCalculator;

	public static void main(String[] args) {
		try {
			long startTime = System.currentTimeMillis();
			validateArguments(args);
			Property.initialize(PROPERTIES_NAMESPACE.NODE);
			(nodesManager = ServerHeartBeat.init()).startup();
			LOGGER.info("Start Node initialize");
			JobRunner jobRunner = createJobRunner();
			List<JobEntity> jobEntities = getJobsFromZKNode();
			for (JobEntity jobEntity : jobEntities) {
				jobRunner.addJob(jobEntity);
			}
			jobRunner.run();
			LOGGER.info("Finished with execution of all jobs");
			jobEntities.clear();
			String buildStartPath = ZKUtils.buildNodePath(NodeUtil.getNodeId()) + PathUtil.FORWARD_SLASH
					+ CommonUtil.ZKJobNodeEnum.FINISH_JOB_MATRIX.name();
			nodesManager.createEphemeralNode(buildStartPath, null);
			long endTime = System.currentTimeMillis();
			LOGGER.info("It took {} seconds of time to execute all jobs.", ((endTime - startTime) / 1000));
			LOGGER.info("ALL JOBS ARE FINISHED");
		} catch (Exception exception) {
			LOGGER.error("Error occured while executing node starter program.", exception);
		}
	}

	/**
	 * To validate the argument command line.
	 * 
	 * @param args
	 * @throws IOException
	 */
	private static void validateArguments(String[] args) throws IOException, FileNotFoundException {
		if (args.length == 1) {
			Property.overrideConfigurationProperties(args[0]);
		} else {
			System.out.println("Please provide the zookeeper configuration file");
			System.exit(1);
		}
	}

	/**
	 * Create the job runner.
	 * 
	 * @return JobRunner
	 * @throws IOException
	 */
	private static JobRunner createJobRunner() throws IOException {
		FieldTypeArrayDataDescription dataDescription = CommonUtil.getConfiguredDataDescription();
		dataDescription.setKeyOrder(Property.getKeyOrder());
		nodeDataStoreIdCalculator = new NodeDataStoreIdCalculator(NodeUtil.getKeyToValueToBucketMap(),
				NodeUtil.getBucketToNodeNumberMap(), NodeUtil.getNodeId(), dataDescription);
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
		String buildPath = ZKUtils.buildNodePath(NodeUtil.getNodeId()) + PathUtil.FORWARD_SLASH
				+ CommonUtil.ZKJobNodeEnum.PUSH_JOB_NOTIFICATION.name();
		Set<LeafBean> leafs = ZKUtils.searchTree(buildPath, null, null);
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
