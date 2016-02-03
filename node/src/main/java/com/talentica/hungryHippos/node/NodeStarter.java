/**
 * 
 */
package com.talentica.hungryHippos.node;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.common.JobRunner;
import com.talentica.hungryHippos.common.TaskEntity;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZKUtils;
import com.talentica.hungryHippos.coordination.domain.LeafBean;
import com.talentica.hungryHippos.coordination.domain.ServerHeartBeat;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
import com.talentica.hungryHippos.resource.manager.domain.ResourceConsumer;
import com.talentica.hungryHippos.resource.manager.domain.ResourceConsumerComparator;
import com.talentica.hungryHippos.resource.manager.services.ResourceConsumerImpl;
import com.talentica.hungryHippos.resource.manager.services.ResourceManager;
import com.talentica.hungryHippos.resource.manager.services.ResourceManagerImpl;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Sharding;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryHippos.storage.NodeDataStoreIdCalculator;
import com.talentica.hungryHippos.utility.CommonUtil;
import com.talentica.hungryHippos.utility.CommonUtil.ZKNodeDeleteSignal;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.Property.PROPERTIES_NAMESPACE;
import com.talentica.hungryHippos.utility.marshaling.DataDescription;
import com.talentica.hungryHippos.utility.marshaling.FieldTypeArrayDataDescription;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * NodeStarter will accept the sharded data and do various operations i.e row
 * count per job and also execution of the aggregation of the data.
 * 
 * Created by debasishc on 1/9/15.
 */
@SuppressWarnings("unchecked")
public class NodeStarter {

	private static DataDescription dataDescription;
	private static String nodeIdFile = "nodeId";
	private static final Logger LOGGER = LoggerFactory.getLogger(NodeStarter.class.getName());
	private static NodesManager nodesManager;
	private static ResourceManager resourceManager;

	private static Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap = null;

	public NodeStarter(DataDescription dataDescription)
			throws IOException, ClassNotFoundException, KeeperException, InterruptedException {
		NodeStarter.dataDescription = dataDescription;
	}

	static {
		try (ObjectInputStream inKeyValueNodeNumberMap = new ObjectInputStream(
				new FileInputStream(new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath() + PathUtil.FORWARD_SLASH
						+ Sharding.keyToValueToBucketMapFile))) {
			keyToValueToBucketMap = (Map<String, Map<Object, Bucket<KeyValueFrequency>>>) inKeyValueNodeNumberMap
					.readObject();
		} catch (IOException | ClassNotFoundException e) {
			LOGGER.info("Unable to read keyValueNodeNumberMap. Please put the file in current directory");
		}
	}

	/**
	 * Read the file nodeId which contains nodeId value.
	 * 
	 * @return NodeId
	 * @throws Exception
	 */
	@SuppressWarnings("resource")
	public static int readNodeId() throws IOException {
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(
					new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath() + PathUtil.FORWARD_SLASH + nodeIdFile)));
			String line = in.readLine();
			return Integer.parseInt(line);
		} catch (IOException exception) {
			LOGGER.info("Unable to read the file for NODE ID. Exiting..");
			throw exception;
		}
	}

	/**
	 * It will open the port to accept the sharded data from client.
	 * 
	 * @param port
	 * @param nodeId
	 * @throws Exception
	 */
	private void startServer(int port, int nodeId) throws Exception {
		LOGGER.info("Start the node");
		EventLoopGroup workerGroup = new NioEventLoopGroup();
		EventLoopGroup bossGroup = new NioEventLoopGroup();

		try {
			final NodeDataStoreIdCalculator nodeDataStoreIdCalculator = new NodeDataStoreIdCalculator(
					keyToValueToBucketMap, nodeId, dataDescription);
			final DataStore dataStore = new FileDataStore(keyToValueToBucketMap.size(), nodeDataStoreIdCalculator,
					dataDescription);
			ServerBootstrap b = new ServerBootstrap();
			b.group(bossGroup, workerGroup);
			b.channel(NioServerSocketChannel.class);
			b.option(ChannelOption.SO_KEEPALIVE, true);
			b.childHandler(new ChannelInitializer<SocketChannel>() {
				@Override
				protected void initChannel(SocketChannel ch) throws Exception {
					ch.pipeline().addLast(new DataReadHandler(dataDescription, dataStore));
				}
			});
			LOGGER.info("binding to port " + port);
			ChannelFuture f = b.bind(port).sync();
			f.channel().closeFuture().sync();
			LOGGER.info("Wait until the connection is closed");

		} finally {
			workerGroup.shutdownGracefully();
			bossGroup.shutdownGracefully();
			LOGGER.info("Connection is gracefully closed");
		}
	}

	public static void main(String[] args) {
		try {
			validateArguments(args);
			Property.setNamespace(PROPERTIES_NAMESPACE.NODE);
			(nodesManager = ServerHeartBeat.init()).startup(ZKNodeDeleteSignal.NODE.name());
			ZKNodeFile serverConfig = ZKUtils.getConfigZKNodeFile(Property.SERVER_CONF_FILE);
			int nodeId = readNodeId();
			String server;
			server = serverConfig.getFileData().getProperty("server." + nodeId);
			int PORT = Integer.valueOf(server.split(":")[1]);

			LOGGER.info("Start Node initialize");
			JobRunner jobRunner = createJobRunner();
			getNodeInitializer(nodesManager).startServer(PORT, nodeId);
			CountDownLatch signal = new CountDownLatch(1);
			waitForStartRowCountSignal(signal);
			signal.await();
			jobRunner.addJobs(getJobsFromZKNode());
			jobRunner.doRowCount();
			List<TaskEntity> workEntities = jobRunner.getWorkEntities();
			LOGGER.info("SIZE OF WORKENTITIES {}", workEntities.size());
			CommonUtil.dumpFileOnDisk("workEntities", workEntities);
			signal = new CountDownLatch(1);
			createZKFinishNodeForRowCount(signal);
			signal.await();

			signal = new CountDownLatch(1);
			waitForStartJobMatrixSignal(signal);
			signal.await();

			signal = new CountDownLatch(1);
			boolean flag = runJobMatrix(jobRunner, workEntities, signal);
			if (flag) {
				signal.await();
				String buildStartPath = ZKUtils.buildNodePath(NodeStarter.readNodeId()) + PathUtil.FORWARD_SLASH
						+ CommonUtil.ZKJobNodeEnum.FINISH_JOB_MATRIX.name();
				nodesManager.createNode(buildStartPath, null);
			}
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
	private static void validateArguments(String[] args) throws IOException {
		if (args.length == 1) {
			try {
				Property.CONFIG_FILE = new FileInputStream(new String(args[0]));
			} catch (FileNotFoundException exception) {
				LOGGER.info("File not found ", exception);
				throw exception;
			}
		} else {
			System.out.println("Please provide the zookeeper configuration file");
			System.exit(1);
		}
	}

	/**
	 * Initialize the node.
	 * 
	 * @param nodesManager
	 * @return NodeStarter
	 * @throws Exception
	 */
	private static NodeStarter getNodeInitializer(NodesManager nodesManager) throws Exception {
		FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription();
		CommonUtil.setDataDescription(dataDescription);
		dataDescription.setKeyOrder(Property.getKeyOrder());
		return new NodeStarter(dataDescription);
	}

	/**
	 * To run the jobs for aggregation or other operations.
	 * 
	 * @param jobRunner
	 * @param signal
	 * @return boolean
	 * @throws Exception
	 */
	private static boolean runJobMatrix(JobRunner jobRunner, List<TaskEntity> workEntities, CountDownLatch signal)
			throws Exception {
		LOGGER.info("STARTING JOB RUNNER MATRIX");
		List<TaskEntity> taskEntities = new ArrayList<>();
		taskEntities.addAll(workEntities);
		jobRunner.clear();
		for (Entry<Integer, List<ResourceConsumer>> entry : getTasksOnPriority(taskEntities).entrySet()) {
			LOGGER.info("RESOURCE INDEX {}", entry.getKey());
			for (ResourceConsumer consumer : entry.getValue()) {
				for (TaskEntity taskEntity : taskEntities) {
					if (taskEntity.getTaskId() == consumer.getResourceRequirement().getResourceId()) {
						jobRunner.addTask(taskEntity);
						taskEntities.remove(taskEntity);
						LOGGER.info("JOB ID {} AND TASK ID {} AND VALUE SET {} AND COUNT {} WILL BE EXECUTED",
								taskEntity.getJob().getJobId(), taskEntity.getTaskId(), taskEntity.getValueSet(),
								taskEntity.getRowCount());
						break;
					}
				}
			}
			jobRunner.run();
			jobRunner.clear();
		}
		signal.countDown();
		return true;
	}

	/**
	 * Create the job runner.
	 * 
	 * @return JobRunner
	 * @throws IOException
	 */
	private static JobRunner createJobRunner() throws IOException {
		FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription();
		CommonUtil.setDataDescription(dataDescription);
		dataDescription.setKeyOrder(Property.getKeyOrder());
		NodeDataStoreIdCalculator nodeDataStoreIdCalculator = new NodeDataStoreIdCalculator(
				NodeStarter.keyToValueToBucketMap, readNodeId(), dataDescription);
		int totalDimensions = Property.getKeyOrder().length;
		FileDataStore dataStore = new FileDataStore(totalDimensions, nodeDataStoreIdCalculator, dataDescription, true);
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
		buildStartPath = ZKUtils.buildNodePath(NodeStarter.readNodeId()) + PathUtil.FORWARD_SLASH
				+ CommonUtil.ZKJobNodeEnum.START_ROW_COUNT.name();
		ZKUtils.waitForSignal(buildStartPath, signal);
	}

	/**
	 * Wait for notification for the start job notification.
	 * 
	 * @param signal
	 * @throws IOException
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	private static void waitForStartJobMatrixSignal(CountDownLatch signal) throws IOException, KeeperException, InterruptedException{
			String buildStartPath = null;
			buildStartPath = ZKUtils.buildNodePath(NodeStarter.readNodeId()) + PathUtil.FORWARD_SLASH + CommonUtil.ZKJobNodeEnum.START_JOB_MATRIX.name();
			ZKUtils.waitForSignal(buildStartPath,signal);
	}

	/**
	 * Push the finish notification for row count per task
	 * 
	 * @param signal
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private static void createZKFinishNodeForRowCount(CountDownLatch signal) throws IOException, InterruptedException {
		String buildPath = ZKUtils.buildNodePath(NodeStarter.readNodeId()) + PathUtil.FORWARD_SLASH
				+ CommonUtil.ZKJobNodeEnum.FINISH_ROW_COUNT.name();
		nodesManager.createNode(buildPath, signal);
		signal.countDown();
	}

	/**
	 * Prioritize the execution of the task based on resource requirement per
	 * task operations i.e row count and calculations.
	 * 
	 * @param taskEntities
	 * @return Map<Integer, List<ResourceConsumer>>
	 */
	private static Map<Integer, List<ResourceConsumer>> getTasksOnPriority(List<TaskEntity> taskEntities) {
		long AVAILABLE_RAM = Long.valueOf(Property.getProperties().getProperty("node.available.ram"));
		resourceManager = new ResourceManagerImpl();
		ResourceConsumer resourceConsumer;
		List<ResourceConsumer> resourceConsumers = new ArrayList<ResourceConsumer>();
		long diskSizeNeeded = 0l;
		for (TaskEntity taskEntity : taskEntities) {
			resourceConsumer = new ResourceConsumerImpl(diskSizeNeeded,
					taskEntity.getJob().getMemoryFootprint(taskEntity.getRowCount()), taskEntity.getTaskId());
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
	private static List<Job> getJobsFromZKNode()
			throws IOException, ClassNotFoundException, InterruptedException, KeeperException {
		String buildPath = ZKUtils.buildNodePath(NodeStarter.readNodeId()) + PathUtil.FORWARD_SLASH
				+ CommonUtil.ZKJobNodeEnum.PUSH_JOB_NOTIFICATION.name();
		Set<LeafBean> leafs = ZKUtils.searchTree(buildPath, null, null);
		LOGGER.info("Leafs size found {}", leafs.size());
		List<Job> jobs = new ArrayList<Job>();
		for (LeafBean leaf : leafs) {
			LOGGER.info("Leaf path {} and name {}", leaf.getPath(), leaf.getName());
			String buildLeafPath = ZKUtils.getNodePath(leaf.getPath(), leaf.getName());
			LOGGER.info("Build path {}", buildLeafPath);
			LeafBean leafObject = ZKUtils.getNodeValue(buildPath, buildLeafPath, leaf.getName(), null);
			Job job = (Job) leafObject.getValue();
			if (job == null)
				continue;
			jobs.add(job);
		}
		LOGGER.info("TOTAL JOBS FOUND {}", jobs.size());
		return jobs;
	}

}
