/**
 * 
 */
package com.talentica.hungryHippos.node;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.Work;
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

	private static ResourceManager resourceManager;

	private static DataStore dataStore;

	private static NodeDataStoreIdCalculator nodeDataStoreIdCalculator;

	public static void main(String[] args) {
		try {
			long startTime = System.currentTimeMillis();
			validateArguments(args);
			Property.initialize(PROPERTIES_NAMESPACE.NODE);
			(nodesManager = ServerHeartBeat.init()).startup();
			waitForStartRowCountSignal();
			executeAllJobs();
			String buildStartPath = ZKUtils.buildNodePath(NodeUtil.getNodeId()) + PathUtil.FORWARD_SLASH
					+ CommonUtil.ZKJobNodeEnum.FINISH_JOB_MATRIX.name();
			nodesManager.createEphemeralNode(buildStartPath, null);
			LOGGER.info("All jobs on this node executed successfully.");
			LOGGER.info("*************** TOTAL TIME TAKEN TO EXECUTE ALL JOBS {} ms *******************",
					(System.currentTimeMillis() - startTime));
		} catch (Exception exception) {
			LOGGER.error("Error occured while executing node starter program.", exception);
		}
	}

	private static void executeAllJobs() throws Exception {
		JobRunner jobRunner = createJobRunner();
		Map<Integer, TaskEntity> taskEntities;
		List<JobEntity> allJobsToBeExecuted = getJobsFromZKNode();
		Iterator<JobEntity> itrTotalJobEntity = allJobsToBeExecuted.iterator();
		List<int[]> processedDims = new ArrayList<>();
		long totalTileElapsedInRowCount = 0l;
		// This while loop is for each job to execute sequentially.
		while (itrTotalJobEntity.hasNext()) {
			JobEntity jobEntity = itrTotalJobEntity.next();
			itrTotalJobEntity.remove();
			// if this dimensions is processed then skip.
			if (isDimsProcessed(processedDims, jobEntity)) {
				continue;
			}
			processedDims.add(jobEntity.getJob().getDimensions());
			long startTimeRowCount = System.currentTimeMillis();
			jobRunner.doRowCount(jobEntity);
			totalTileElapsedInRowCount = totalTileElapsedInRowCount + (System.currentTimeMillis() - startTimeRowCount);
			taskEntities = jobRunner.getTaskEntities();
			// if no reducers, just skip.
			if (taskEntities.size() == 0)
			{
				continue; 
			}
			Iterator<JobEntity> allJobsToBeExecutedIterator = allJobsToBeExecuted.iterator();
			Map<Integer, TaskEntity> tasksEntityForDiffIndex = new HashMap<>();
			// To find the jobs having reducers on the same dimensions.
			while (allJobsToBeExecutedIterator.hasNext()) {
				JobEntity currentJob = allJobsToBeExecutedIterator.next();
				Iterator<TaskEntity> tasksPerJobItr = taskEntities.values().iterator();
				// Identify the reducers and create the another reducer for
				// different for same dimension BUT different for index.
				while (tasksPerJobItr.hasNext()) {
					TaskEntity takEntity = tasksPerJobItr.next();
					if (Arrays.equals(currentJob.getJob().getDimensions(),
							takEntity.getJobEntity().getJob().getDimensions())) {
						Work work = currentJob.getJob().createNewWork();
						TaskEntity newTaskEntity = takEntity.clone();
						newTaskEntity.setWork(work);
						newTaskEntity.setJobEntity(currentJob);
						tasksEntityForDiffIndex.put(newTaskEntity.getTaskId(), newTaskEntity);
					}
				}
			}
			taskEntities.putAll(tasksEntityForDiffIndex);
			LOGGER.info("SIZE OF TASKS {}", taskEntities.size());
			LOGGER.info("JOB RUNNER MATRIX STARTED");
			runJobMatrix(jobRunner, taskEntities);
			LOGGER.info("FINISHED JOB RUNNER MATRIX");
			jobRunner.clear();
			taskEntities.clear();
			tasksEntityForDiffIndex.clear();
		}
		processedDims.clear();
		LOGGER.info("TOTAL TIME TAKEN FOR ROW COUNTS OF ALL JOBS {} ms", totalTileElapsedInRowCount);
	}

	private static boolean isDimsProcessed(List<int[]> processedDims, JobEntity jobEntity) {
		boolean dimsProcessed = false;
		Iterator<int[]> dimsItr = processedDims.iterator();
		while (dimsItr.hasNext()) {
			int[] dims = dimsItr.next();
			if (Arrays.equals(dims, jobEntity.getJob().getDimensions())) {
				dimsProcessed = true;
				break;
			}
		}
		return dimsProcessed;
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
	 * To run the jobs for aggregation or other operations.
	 * 
	 * @param jobRunner
	 * @param signal
	 * @return boolean
	 * @throws Exception
	 */
	private static boolean runJobMatrix(JobRunner jobRunner, Map<Integer, TaskEntity> workEntities) throws Exception {
		List<TaskEntity> listOfTaskEntities = new ArrayList<>();
		listOfTaskEntities.addAll(workEntities.values());
		for (Entry<Integer, List<ResourceConsumer>> entry : getTasksOnPriority(listOfTaskEntities).entrySet()) {
			LOGGER.debug("RESOURCE INDEX {}", entry.getKey());
			for (ResourceConsumer consumer : entry.getValue()) {
				Integer resourceId = consumer.getResourceRequirement().getResourceId();
				TaskEntity taskEntity = workEntities.get(resourceId);
				jobRunner.addTask(taskEntity);
				workEntities.remove(resourceId);
				LOGGER.debug("JOB ID {} AND TASK ID {} AND VALUE SET {} AND COUNT {} WILL BE EXECUTED",
						taskEntity.getJobEntity().getJobId(), taskEntity.getTaskId(), taskEntity.getValueSet(),
						taskEntity.getRowCount());
			}
		}
		LOGGER.info("BATCH EXECUTION STARTED");
		long startTime = System.currentTimeMillis();
		jobRunner.run();
		LOGGER.info("BATCH COMPLETION TIME {} ms", (System.currentTimeMillis() - startTime));
		return true;
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
	private static void waitForStartRowCountSignal() throws KeeperException, InterruptedException, IOException {
		CountDownLatch signal = new CountDownLatch(1);
		String buildStartPath = null;
		buildStartPath = ZKUtils.buildNodePath(NodeUtil.getNodeId()) + PathUtil.FORWARD_SLASH
				+ CommonUtil.ZKJobNodeEnum.START_ROW_COUNT.name();
		ZKUtils.waitForSignal(buildStartPath, signal);
		signal.await();
	}

	/**
	 * Prioritize the execution of the task based on resource requirement per
	 * task operations i.e row count and calculations.
	 * 
	 * @param taskEntities
	 * @return Map<Integer, List<ResourceConsumer>>
	 */
	private static Map<Integer, List<ResourceConsumer>> getTasksOnPriority(List<TaskEntity> taskEntities) {
		long AVAILABLE_RAM = Long.valueOf(Property.getPropertyValue("node.available.ram").toString());
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
