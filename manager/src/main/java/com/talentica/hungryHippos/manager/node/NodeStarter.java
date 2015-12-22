/**
 * 
 */
package com.talentica.hungryHippos.manager.node;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.accumulator.Job;
import com.talentica.hungryHippos.accumulator.JobRunner;
import com.talentica.hungryHippos.accumulator.testJobs.TestJob;
import com.talentica.hungryHippos.node.DataReadHandler;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryHippos.storage.NodeDataStoreIdCalculator;
import com.talentica.hungryHippos.utility.CommonUtil;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.ZKNodeName;
import com.talentica.hungryHippos.utility.marshaling.DataDescription;
import com.talentica.hungryHippos.utility.marshaling.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.utility.memory.MemoryCalculator;
import com.talentica.hungryHippos.utility.zookeeper.ServerHeartBeat;
import com.talentica.hungryHippos.utility.zookeeper.ZKNodeFile;
import com.talentica.hungryHippos.utility.zookeeper.ZKUtils;
import com.talentica.hungryHippos.utility.zookeeper.manager.NodesManager;

/**
 * 
 * 
 * Created by debasishc on 1/9/15.
 */
public class NodeStarter {

    private static Map<String,Map<Object, Node>> keyValueNodeNumberMap ;
    private static DataDescription dataDescription;
    private static String nodeIdFile = "nodeId";
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeStarter.class.getName());
    private static NodesManager nodesManager = null;
    @SuppressWarnings("unchecked")
	public NodeStarter(String keyValueNodeNumberMapFile, DataDescription dataDescription, NodesManager nodesManager) throws IOException, ClassNotFoundException, KeeperException, InterruptedException {
    	NodeStarter.dataDescription = dataDescription;
    	ZKNodeFile zkNodeFile = ZKUtils.getZKNodeFile(nodesManager, keyValueNodeNumberMapFile);
        NodeStarter.keyValueNodeNumberMap = (Map<String, Map<Object, Node>>) zkNodeFile.getObj();
    }
    @SuppressWarnings("resource")
	public static int readNodeId() throws Exception{
        BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath()+PathUtil.FORWARD_SLASH+nodeIdFile)));
        String line = in.readLine();
        return Integer.parseInt(line);
    }
    private void startServer(int port,int nodeId) throws Exception {
    	LOGGER.info("\n\t Start the node");
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
            System.out.println("binding to port "+port);
            ChannelFuture f = b.bind(port).sync();
            f.channel().closeFuture().sync();
            LOGGER.info("\n\tWait until the connection is closed");
           
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
            LOGGER.info("\n\t Connection is gracefully closed");
        }
    }

	public static void main(String[] args) {
		try {
			nodesManager = ServerHeartBeat.init();
		} catch (Exception e1) {
			LOGGER.info("\n\tUnable to start the nodeManager");
			return;
		}
		ZKNodeFile serverConfig = ZKUtils.getZKNodeFile(nodesManager,Property.SERVER_CONF_FILE);
		int nodeId = 0;
		try {
			nodeId = readNodeId();
		} catch (Exception e) {
			e.printStackTrace();
		}
		String server;
		server = serverConfig.getFileData().getProperty("server." + nodeId);
		int PORT = Integer.valueOf(server.split(":")[1]);
		try {
			LOGGER.info("\n\tStart Node initialize");
			getNodeInitializer(nodesManager).startServer(PORT, nodeId);
			
			
			NodeDataStoreIdCalculator nodeDataStoreIdCalculator = new NodeDataStoreIdCalculator(
					keyValueNodeNumberMap, NodeStarter.readNodeId(),
					dataDescription);
			int totalDimensions = Integer.valueOf(Property.getProperties()
					.getProperty("total.dimensions"));
			
			
			FileDataStore dataStore = new FileDataStore(totalDimensions,
					nodeDataStoreIdCalculator, dataDescription, true);
			LOGGER.info("\n\tStart the job runner");
			
			
			JobRunner jobRunner = new JobRunner(dataDescription, dataStore,
					keyValueNodeNumberMap);
			
			runJobMatrix(jobRunner);
			
			
			Map<Integer,Long> jobIdMemoMap = new MemoryCalculator(jobRunner.getRowCountByJobId()).getJobMemoryAlloc();
			System.out.println("Total Memory(BYTE) of the job wise :: "+jobIdMemoMap);
			
			putJobDataSize(jobRunner,jobIdMemoMap);
			
			putJobStatisticsZknode(jobIdMemoMap);
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	private static NodeStarter getNodeInitializer(NodesManager nodesManager) throws Exception{
        FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription();
        CommonUtil.setDataDescription(dataDescription);
        dataDescription.setKeyOrder(new String[]{"key1","key2","key3"});
        return new NodeStarter(ZKNodeName.keyValueNodeNumberMap, dataDescription,nodesManager);
    }
	
	private static void runJobMatrix(JobRunner jobRunner){
    	LOGGER.info("\n\t Start the job runner matrix");
   	 int numMetrix = 0;
   	 int jobId = 0;
        for(int i=0;i<3;i++){
            jobRunner.addJob(new TestJob(new int[]{i}, i, 6,jobId++));
            jobRunner.addJob(new TestJob(new int[]{i}, i, 7,jobId++));
            numMetrix+=2;
            for(int j=i+1;j<5;j++){
                jobRunner.addJob(new TestJob(new int[]{i,j}, i, 6,jobId++));
                jobRunner.addJob(new TestJob(new int[]{i,j}, j, 7,jobId++));
                numMetrix+=2;
                for(int k=j+1;k<5;k++){
                    jobRunner.addJob(new TestJob(new int[]{i,j,k}, i, 6,jobId++));
                    jobRunner.addJob(new TestJob(new int[]{i,j,k}, j, 7,jobId++));
                    numMetrix+=2;
                }
            }
        }
        System.out.println(numMetrix);
       jobRunner.run();
   }
	
	private static void putJobDataSize(JobRunner jobRunner,Map<Integer,Long> jobIdMemoMap){
		for(Job job : jobRunner.getJobs()){
			Long memory = jobIdMemoMap.get(job.getJobId());
			if(memory == null || memory == 0l){
				continue;
			}
			job.putDataSize(memory);
		}
		
	}
	
	private static void putJobStatisticsZknode(Map<Integer,Long> jobIdMemoMap) throws Exception{
		ZKNodeFile jobIdMemoMapZkfile = new ZKNodeFile(
				String.valueOf(NodeStarter.readNodeId()), null,
				jobIdMemoMap);
		nodesManager.saveConfigFileToZNode(jobIdMemoMapZkfile);
	}

}
