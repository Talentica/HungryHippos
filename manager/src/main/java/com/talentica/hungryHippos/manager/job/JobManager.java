package com.talentica.hungryHippos.manager.job;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.accumulator.DataProvider;
import com.talentica.hungryHippos.accumulator.Job;
import com.talentica.hungryHippos.accumulator.JobRunner;
import com.talentica.hungryHippos.accumulator.testJobs.TestJob;
import com.talentica.hungryHippos.sharding.KeyCombination;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.Sharding;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryHippos.storage.NodeDataStoreIdCalculator;
import com.talentica.hungryHippos.utility.CommonUtil;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.ZKNodeName;
import com.talentica.hungryHippos.utility.marshaling.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.utility.zookeeper.Server;
import com.talentica.hungryHippos.utility.zookeeper.ServerHeartBeat;
import com.talentica.hungryHippos.utility.zookeeper.ZKNodeFile;
import com.talentica.hungryHippos.utility.zookeeper.ZKUtils;
import com.talentica.hungryHippos.utility.zookeeper.manager.NodesManager;

public class JobManager {

	Set<Server> regServer = null;
	static String configPath;
	static NodesManager nodesManager;
	private static Map<String, Map<Object, Node>> keyValueNodeNumberMap;
	private static final Logger LOGGER = LoggerFactory.getLogger(JobManager.class.getName());
	private List<Job> jobList = new ArrayList<Job>();
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			LOGGER.info("You have not provided external config.properties file. Default config.properties file will be use internally");
		} else if (args.length == 1) {
			Property.CONFIG_FILE = new FileInputStream(new String(args[0]));
		}
		ServerHeartBeat heartBeat = new ServerHeartBeat();
		/*String root = Property.getProperties().getProperty(
				"zookeeper.namespace_path");*/
		String tickTime = Property.getProperties().getProperty("tick.time");
		LOGGER.info("\n\tDeleting All nodes on zookeeper");
		//heartBeat.deleteAllNodes(PathUtil.FORWARD_SLASH + root);
		(nodesManager = ServerHeartBeat.init()).startup();
		ZKNodeFile serverConfigFile = new ZKNodeFile(Property.SERVER_CONF_FILE,
				Property.loadServerProperties());
		nodesManager.saveConfigFileToZNode(serverConfigFile);

		LOGGER.info("\n\tSHARDING STARTED.....");
		Sharding.doSharding();   // do the sharding
		LOGGER.info("\n\tSave the KeyValueMap configuration on zookeeper node");
		/* To save keyValueNodeNumberMap data to ZKNode */
		try (ObjectInputStream inKeyValueNodeNumberMap = new ObjectInputStream(
				new FileInputStream(
						new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath()
								+ PathUtil.FORWARD_SLASH
								+ ZKNodeName.keyValueNodeNumberMap))) {
			keyValueNodeNumberMap = (Map<String, Map<Object, Node>>) inKeyValueNodeNumberMap
					.readObject();
			ZKNodeFile saveKeyValueNodeNumberMap = new ZKNodeFile(
					ZKNodeName.keyValueNodeNumberMap, null,
					keyValueNodeNumberMap);
			nodesManager.saveConfigFileToZNode(saveKeyValueNodeNumberMap);

		} catch (Exception e) {
			e.printStackTrace();
		}
			LOGGER.info("\n\tPublish the data across the nodes");
			DataProvider.publishDataToNodes(nodesManager);
		
		List<Server> regServer = heartBeat.getMonitoredServers();
		LOGGER.info("\n\t\t********STARTING TO PING THE SERVER********");
		while (true) {
			for (Server server : regServer) {
				heartBeat.startPinging(server);
				Thread.sleep(Long.valueOf(tickTime));
			}
		}

	}
	
	private void jobMemoCalc() throws IOException{
		FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription();
        CommonUtil.setDataDescription(dataDescription);
        dataDescription.setKeyOrder(new String[]{"key1","key2","key0"});
        for(int nodeId = 0 ; nodeId < 10 ; nodeId++ ){
		NodeDataStoreIdCalculator nodeDataStoreIdCalculator = new NodeDataStoreIdCalculator(
				keyValueNodeNumberMap, nodeId, dataDescription);
		int totalDimensions = Integer.valueOf(Property.getProperties()
				.getProperty("total.dimensions"));
		FileDataStore dataStore = new FileDataStore(totalDimensions,
				nodeDataStoreIdCalculator, dataDescription, true);
		LOGGER.info("\n\tStart the job runner");
		JobRunner jobRunner = new JobRunner(dataDescription, dataStore,
				keyValueNodeNumberMap);
		runJobMatrix(jobRunner);
        }
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
   }
	
	private void scheduleNodeJob(JobRunner jobRunner,List<Node> nodes){
		for(Job job : jobRunner.getJobs()){
			this.jobList.add(job);
		}
	}
	
	private void receiveStatisticsNodeJobs() throws IOException{
		Map<String, Map<Object, Node>> keyValueNodeNumberMap;
		int totalNodes = Property.loadServerProperties().size();
		ZKNodeFile zkNodeFile = ZKUtils.getZKNodeFile(nodesManager, ZKNodeName.keyValueNodeNumberMap);
		keyValueNodeNumberMap = (zkNodeFile==null) ? null : (Map<String, Map<Object, Node>>)zkNodeFile.getObj();
		Map<Object, Node> mapValue = (Map<Object, Node>) keyValueNodeNumberMap.values();
		Set<Node> nodes = new HashSet<>(mapValue.values());
		for (Node node : nodes) {
			ZKNodeFile zkNodeFileId = ZKUtils.getZKNodeFile(nodesManager,String.valueOf(node.getNodeId()));
			Object obj = zkNodeFileId.getObj();
			Map<Integer, Long> jobIdMemoMap = (Map<Integer, Long>) obj;
			NodeJobsExecutor nodeJobsExecutor = new NodeJobsExecutor(node,nodesManager);
			for (Integer jobIdKey : jobIdMemoMap.keySet()) {
				nodeJobsExecutor.addJob(this.jobList.get(jobIdKey));
			}
			nodeJobsExecutor.createNodeJobExecutor();
		}
		
	}

}
