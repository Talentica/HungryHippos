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
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.common.JobComparator;
import com.talentica.hungryHippos.common.JobEntity;
import com.talentica.hungryHippos.common.JobRunner;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZKUtils;
import com.talentica.hungryHippos.coordination.domain.LeafBean;
import com.talentica.hungryHippos.coordination.domain.ServerHeartBeat;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryHippos.storage.NodeDataStoreIdCalculator;
import com.talentica.hungryHippos.utility.CommonUtil;
import com.talentica.hungryHippos.utility.CommonUtil.ZKNodeDeleteSignal;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.Property.PROPERTIES_NAMESPACE;
import com.talentica.hungryHippos.utility.ZKNodeName;
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
 * NodeStarter will accept the sharded data and do various operations i.e row count per job and also execution of the aggregation of the data. 
 * 
 * Created by debasishc on 1/9/15.
 */
public class NodeStarter {

    private static Map<String,Map<Object, Node>> keyValueNodeNumberMap ;
    private static DataDescription dataDescription;
    private static String nodeIdFile = "nodeId";
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeStarter.class.getName());
    private static NodesManager nodesManager = null;
    
	public NodeStarter(DataDescription dataDescription) throws IOException, ClassNotFoundException, KeeperException, InterruptedException {
    	NodeStarter.dataDescription = dataDescription;
    	setKeyValueNodeNumberMap();
    }
    
    
    /**
     * Read the file nodeId which contains nodeId value.
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
    private void startServer(int port,int nodeId) throws Exception {
    	LOGGER.info("Start the node");
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        EventLoopGroup bossGroup = new NioEventLoopGroup();

        try {
            //need to setup this
            final NodeDataStoreIdCalculator nodeDataStoreIdCalculator
                    = new NodeDataStoreIdCalculator(keyValueNodeNumberMap,nodeId,dataDescription);
            final DataStore dataStore
                    = new FileDataStore(keyValueNodeNumberMap.size(),nodeDataStoreIdCalculator,dataDescription);

            ServerBootstrap b = new ServerBootstrap(); 
            b.group(bossGroup,workerGroup); 
            b.channel(NioServerSocketChannel.class); 
            b.option(ChannelOption.SO_KEEPALIVE, true); 
            b.childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(new DataReadHandler(dataDescription, dataStore));                    
                }
            });
            LOGGER.info("binding to port "+port);
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
			CountDownLatch signal = new CountDownLatch(1);
			LOGGER.info("Start Node initialize");
			getNodeInitializer(nodesManager).startServer(PORT, nodeId);
			JobRunner jobRunner = getJobRunnerFromZKnode(nodeId);
			List<JobEntity> jobEntities = jobRunner.getJobIdJobEntityMap();
			CommonUtil.dumpFileOnDisk("jobEntities", jobEntities);
			putJobStatisticsZknode(jobEntities);
			boolean flag = runJobMatrix(jobRunner, signal);
			if (flag) {
				signal.await();
				String buildStartPath = ZKUtils.buildNodePath(NodeStarter.readNodeId()) + PathUtil.FORWARD_SLASH
						+ CommonUtil.ZKJobNodeEnum.FINISH.name();
				nodesManager.createNode(buildStartPath, null);
			}
		} catch (Exception exception) {
			LOGGER.error("Error occured while executing node starter program.", exception);
		}
	}


	private static void validateArguments(String[] args) throws FileNotFoundException {
		if (args.length == 1) {
			try {
				Property.CONFIG_FILE = new FileInputStream(new String(args[0]));
			} catch (FileNotFoundException exception) {
				LOGGER.info("File not found ", exception);
				throw exception;
			}
		}else{
			System.out.println("Please provide the zookeeper configuration file");
			System.exit(1);
		}
	}
	
	/**
	 * Initialize the node.
	 * 
	 * @param nodesManager
	 * @return
	 * @throws Exception
	 */
	private static NodeStarter getNodeInitializer(NodesManager nodesManager) throws Exception{
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
	 * @return
	 * @throws Exception
	 */
	private static boolean runJobMatrix(JobRunner jobRunner,CountDownLatch signal) throws Exception{
    	LOGGER.info("STARTING JOB RUNNER MATRIX");
			List<JobEntity> jobEntities = getJobsFromZKNode();
			if(jobEntities.isEmpty()){
				LOGGER.info("There is no jobs to run. Exiting.");
				return false;
			}
			
			jobRunner.clearJobList();
			
			for (JobEntity jobEntity : jobEntities) {
				LOGGER.info("JOB ID :: {}",jobEntity.getJob().getJobId());
				jobRunner.addJob(jobEntity.getJob());
			}
			
			jobRunner.run();
			signal.countDown();
			return true;
   }
	
	
	
	/**
	 * To put the JOBs statistics i.e row count per job on, ZK NODE.
	 * 
	 * @param jobIdJobEntityMap
	 * @throws Exception
	 */
	private static void putJobStatisticsZknode(List<JobEntity> jobEntities)
			throws Exception {
		CountDownLatch signal = new CountDownLatch(jobEntities.size());
		for (JobEntity jobEntity : jobEntities) {
			ZKNodeFile jobEntitiesZkfile = new ZKNodeFile(
					String.valueOf("_node" + NodeStarter.readNodeId()
							+ PathUtil.FORWARD_SLASH + "_job"
							+ jobEntity.getJob().getJobId()), null, jobEntity);
			nodesManager.saveConfigFileToZNode(jobEntitiesZkfile, signal);
		}
		signal.await();
		String buildConfigPath = nodesManager.buildConfigPath("_node" + NodeStarter.readNodeId());
		nodesManager.createNode(String.valueOf(buildConfigPath + PathUtil.FORWARD_SLASH	+ "FINISH"),null);
	}
	
	
	
	/**
	 * To get the list of jobs from the ZK NODE for operation.
	 * 
	 * @return List<JobEntity>
	 * @throws Exception
	 */
	private static List<JobEntity> getJobsFromZKNode() throws Exception{
		CountDownLatch signal = new CountDownLatch(1);
		String buildStartPath =  ZKUtils.buildNodePath(NodeStarter.readNodeId()) + PathUtil.FORWARD_SLASH + CommonUtil.ZKJobNodeEnum.START.name();
		ZKUtils.isNodeExists(buildStartPath,signal);
		signal.await();
		
		String buildPath = ZKUtils.buildNodePath(NodeStarter.readNodeId()) + PathUtil.FORWARD_SLASH + CommonUtil.ZKJobNodeEnum.PUSH_JOB_NOTIFICATION.name();
		LOGGER.info(" Build Path is {}",buildPath);
		
		signal = new CountDownLatch(1);
		ZKUtils.isNodeExists(buildStartPath,signal);
		signal.await();
		
		Set<LeafBean> jobBeans = ZKUtils.searchTree(buildPath, null,null);
			jobBeans = ZKUtils.searchTree(buildPath, null,null);
		LOGGER.info("No. of jobs found {}",jobBeans.size());
		
		
		List<JobEntity> jobEntities = new ArrayList<>();
		for(LeafBean leaf : jobBeans){
			JobEntity jobEntity = (JobEntity)leaf.getValue();
			jobEntities.add(jobEntity);
		}
		
		Collections.sort(jobEntities,new JobComparator());
		return jobEntities;
	}
	
	
		
	/**
	 * Get the JobRunner instances from the ZK node to perform the various operations i.e row count and calculations.
	 * 
	 * @param nodeId
	 * @return JobRunner
	 * @throws InterruptedException
	 */
	private static JobRunner getJobRunnerFromZKnode(int nodeId) throws InterruptedException{
		ZKNodeFile zkNodeFile = null;
		while(zkNodeFile == null){
			zkNodeFile = ZKUtils.getConfigZKNodeFile("_node"+nodeId);
		}
		return (JobRunner)zkNodeFile.getObj();
	}
	
	
	/**
	 * Get the keyValueNodeNumberMap from local storage.
	 * 
	 */
	@SuppressWarnings("unchecked")
	private static void setKeyValueNodeNumberMap(){
		try (ObjectInputStream inKeyValueNodeNumberMap = new ObjectInputStream(
				new FileInputStream(
						new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath()
								+ PathUtil.FORWARD_SLASH
								+ ZKNodeName.keyValueNodeNumberMap))) {
			NodeStarter.keyValueNodeNumberMap = (Map<String, Map<Object, Node>>) inKeyValueNodeNumberMap.readObject();
		} catch (IOException|ClassNotFoundException e) {
			LOGGER.info("Unable to read keyValueNodeNumberMap. Please put the file in current directory");
		}
	}
	
}
