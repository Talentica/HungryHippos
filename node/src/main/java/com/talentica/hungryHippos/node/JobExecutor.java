/**
 * 
 */
package com.talentica.hungryHippos.node;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.common.JobRunner;
import com.talentica.hungryHippos.common.TaskEntity;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZKUtils;
import com.talentica.hungryHippos.coordination.domain.LeafBean;
import com.talentica.hungryHippos.coordination.domain.ServerHeartBeat;
import com.talentica.hungryHippos.resource.manager.domain.ResourceConsumer;
import com.talentica.hungryHippos.resource.manager.domain.ResourceConsumerComparator;
import com.talentica.hungryHippos.resource.manager.services.ResourceConsumerImpl;
import com.talentica.hungryHippos.resource.manager.services.ResourceManager;
import com.talentica.hungryHippos.resource.manager.services.ResourceManagerImpl;
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

	private static final Stopwatch STOPWATCH = new Stopwatch();

	private static ResourceManager resourceManager;

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
			CountDownLatch signal = new CountDownLatch(1);
			waitForStartRowCountSignal(signal);
			signal.await();
			List<JobEntity> jobEntities = getJobsFromZKNode();
			for (JobEntity jobEntity : jobEntities) {
				jobRunner.run(jobEntity);
			}
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
		dataStore = new FileDataStore(NodeUtil.getKeyToValueToBucketMap().size(), nodeDataStoreIdCalculator,
				dataDescription, true);
		return new JobRunner(dataDescription, dataStore);
	}

	/**
	 * Wait for notification for the start row count.
	 * 
	 * @param signal
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private static void waitForStartRowCountSignal(CountDownLatch signal)
			throws KeeperException, InterruptedException, IOException {
		String buildStartPath = null;
		buildStartPath = ZKUtils.buildNodePath(NodeUtil.getNodeId()) + PathUtil.FORWARD_SLASH
				+ CommonUtil.ZKJobNodeEnum.START_ROW_COUNT.name();
		ZKUtils.waitForSignal(buildStartPath, signal);
	}

	/**
	 * Prioritize the execution of the task based on resource requirement per
	 * task operations i.e row count and calculations.
	 * 
	 * @param taskEntities
	 * @return Map<Integer, List<ResourceConsumer>>
	 */
	private static Map<Integer, List<ResourceConsumer>> getTasksOnPriority(Collection<TaskEntity> taskEntities) {
		long AVAILABLE_RAM = Long.valueOf(Property.getProperties().getProperty("node.available.ram"));
		resourceManager = new ResourceManagerImpl();
		ResourceConsumer resourceConsumer;
		List<ResourceConsumer> resourceConsumers = new ArrayList<ResourceConsumer>();
		long diskSizeNeeded = 0l;
		for (TaskEntity taskEntity : taskEntities) {
			resourceConsumer = new ResourceConsumerImpl(diskSizeNeeded,
					taskEntity.getJobEntity().getJob().getMemoryFootprint(taskEntity.getRowCount()),
					taskEntity.getTaskId());
			resourceConsumers.add(resourceConsumer);
		}
		Collections.sort(resourceConsumers, new ResourceConsumerComparator());
		return resourceManager.getIterationWiseResourceConsumersToAllocateResourcesTo(0l, AVAILABLE_RAM,
				resourceConsumers);
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
