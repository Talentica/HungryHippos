package com.talentica.hungryHippos.manager.job;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.accumulator.DataProvider;
import com.talentica.hungryHippos.accumulator.Job;
import com.talentica.hungryHippos.accumulator.JobRunner;
import com.talentica.hungryHippos.accumulator.testJobs.TestJob;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.Sharding;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryHippos.storage.NodeDataStoreIdCalculator;
import com.talentica.hungryHippos.utility.CommonUtil;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.ZKNodeName;
import com.talentica.hungryHippos.utility.marshaling.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.utility.marshaling.Reader;
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
	private static final Logger LOGGER = LoggerFactory.getLogger(JobManager.class);
	private static List<Job> jobList = new ArrayList<Job>();
	private final static int noOfNodes = Integer.valueOf(Property.getProperties().getProperty("total.nodes"));
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			LOGGER.info("You have not provided external config.properties file. Default config.properties file will be use internally");
		} else if (args.length == 1) {
			Property.CONFIG_FILE = new FileInputStream(new String(args[0]));
		}
		//ServerHeartBeat heartBeat = new ServerHeartBeat();
		//String tickTime = Property.getProperties().getProperty("tick.time");
		(nodesManager = ServerHeartBeat.init()).startup();
		ZKNodeFile serverConfigFile = new ZKNodeFile(Property.SERVER_CONF_FILE,
				Property.loadServerProperties());
		
		nodesManager.saveConfigFileToZNode(serverConfigFile);

		LOGGER.info("\n\tSHARDING STARTED.....");
		
		Sharding.doSharding(getReaderForSharding(), noOfNodes); // do the
		
		LOGGER.info("\n\t SHARDING DONE!!");
		LOGGER.info("\n\tPutting the keyValueNodeNumberMap configuration on zookeeper node");
		
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

		} catch (Exception exception) {
			LOGGER.error("Error occurred while starting up job manager.", exception);
		}
			LOGGER.info("\n\tPUBLISH DATA ACROSS THE NODES");
			DataProvider.publishDataToNodes(nodesManager);
			LOGGER.info("\n\tDATA PUBLISHED SUCCESSFULLY!");
			
			LOGGER.info("\n\tSENDING JOBRUNNER TO NODES TO CALCULATE ROWCOUNT/JOB");
			createAndSendJobRunnerZKNode();
			LOGGER.info("\n\tJOB RUNNER SENT");
			
			LOGGER.info("\n\n\n\t*****SPAWNING THE JOBS ACROSS NODES****\n\n\n");
			executeJobsOnNodes();
			LOGGER.info("\n\n\n\t FINISHED!\n\n\n");
			
			
		
		//List<Server> regServer = heartBeat.getMonitoredServers();
		
		//LOGGER.info("\n\t\t********STARTING TO PING THE SERVER********");
		/*while (true) {
			for (Server server : regServer) {
				String buildFinishPath =  ZKUtils.buildNodePath(server.getId()) + PathUtil.FORWARD_SLASH + CommonUtil.ZKJobNodeEnum.FINISH.name();
				nodesManager.isPathExists(buildFinishPath, null);
				heartBeat.startPinging(server);
				Thread.sleep(Long.valueOf(tickTime));
			}
		}*/

	}

	private static Reader getReaderForSharding() throws IOException {
		final String inputFile = Property.getProperties().getProperty("input.file");
		com.talentica.hungryHippos.utility.marshaling.FileReader fileReader = new com.talentica.hungryHippos.utility.marshaling.FileReader(
				inputFile);
		fileReader.setNumFields(9);
		fileReader.setMaxsize(25);
		return fileReader;
	}
	
	private static void executeJobsOnNodes() throws IOException, InterruptedException, KeeperException, ClassNotFoundException{
		long startTime = new Date().getTime();
		Map<Integer,Node> nodeIdNodeMap = getNodesMap();
		LOGGER.info("No. of nodes :: " + nodeIdNodeMap.size());
		for (Integer nodeId : nodeIdNodeMap.keySet()) {
			Map<Integer, Long> jobIdRowCountMap = getRowCountZKNode(nodeId);
			NodeJobsExecutor nodeJobsExecutor = new NodeJobsExecutor(nodeIdNodeMap.get(nodeId),nodesManager);
			for (Integer jobIdKey : jobIdRowCountMap.keySet()) {
				Job job = jobList.get(jobIdKey);
				job.addDataSize(jobIdRowCountMap.get(jobIdKey));
				nodeJobsExecutor.addJob(job);
			}
			nodeJobsExecutor.createNodeJobExecutor();
			nodeJobsExecutor.scheduleJobNode();
		}
		
		LOGGER.info("ALL JOBS ARE FINISHED!");
		LOGGER.info("Time taken(Sec) in node create ::" + (new Date().getTime()-startTime)/1000);
		
	}
	
	private static void createAndSendJobRunnerZKNode() throws IOException, InterruptedException, KeeperException{
		boolean flag = false;
		int totalNods = Integer.valueOf(Property.getProperties().getProperty("total.nodes"));
		FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription();
        CommonUtil.setDataDescription(dataDescription);
        dataDescription.setKeyOrder(new String[]{"key1","key2","key3"});
        
		for (int nodeId = 0; nodeId < totalNods; nodeId++) {
			NodeDataStoreIdCalculator nodeDataStoreIdCalculator = new NodeDataStoreIdCalculator(
					keyValueNodeNumberMap, nodeId, dataDescription);
			int totalDimensions = Integer.valueOf(Property.getProperties()
					.getProperty("total.dimensions"));
			FileDataStore dataStore = new FileDataStore(totalDimensions,
					nodeDataStoreIdCalculator, dataDescription, true);
			JobRunner jobRunner = new JobRunner(dataDescription, dataStore,
					keyValueNodeNumberMap);

			createJobRunner(jobRunner);

			sendJobsConfiguration(jobRunner, nodeId);

			addJobs(jobRunner, flag);
			flag = true;
		}
	}
	
	private static void createJobRunner(JobRunner jobRunner) throws IOException{
   	 int jobId = 0;
        for(int i=0;i<3;i++){
            jobRunner.addJob(new TestJob(new int[]{i}, i, 6,jobId++));
            jobRunner.addJob(new TestJob(new int[]{i}, i, 7,jobId++));
            for(int j=i+1;j<5;j++){
                jobRunner.addJob(new TestJob(new int[]{i,j}, i, 6,jobId++));
                jobRunner.addJob(new TestJob(new int[]{i,j}, j, 7,jobId++));
                for(int k=j+1;k<5;k++){
                    jobRunner.addJob(new TestJob(new int[]{i,j,k}, i, 6,jobId++));
                    jobRunner.addJob(new TestJob(new int[]{i,j,k}, j, 7,jobId++));
                }
            }
        }
   }
	
	private static void addJobs(JobRunner jobRunner,boolean flag){
		if (!flag) {
			for (Job job : jobRunner.getJobs()) {
				jobList.add(job);
			}
		}
	}
	
	
	
	private static void sendJobsConfiguration(JobRunner jobRunner,int nodeId) throws IOException{
		ZKNodeFile jobConfigFile = new ZKNodeFile("_node"+nodeId, null, new Object[]{jobRunner});
		nodesManager.saveConfigFileToZNode(jobConfigFile);
	}
	
	
	@SuppressWarnings("unchecked")
	private static Map<Integer,Node> getNodesMap(){
		Map<String, Map<Object, Node>> keyValueNodeNumberMap;
		ZKNodeFile zkNodeFile = ZKUtils.getConfigZKNodeFile(ZKNodeName.keyValueNodeNumberMap);
		keyValueNodeNumberMap = (zkNodeFile==null) ? null : (Map<String, Map<Object, Node>>)zkNodeFile.getObj();
		Map<Integer,Node> nodeIdNodeMap = new HashMap<Integer, Node>();
		for(String key : keyValueNodeNumberMap.keySet()){
			Map<Object, Node> mapNode = keyValueNodeNumberMap.get(key);
			for(Object objKey : mapNode.keySet()){
				nodeIdNodeMap.put(mapNode.get(objKey).getNodeId(), mapNode.get(objKey));
			}
		}
		return nodeIdNodeMap;
	}
	
	@SuppressWarnings("unchecked")
	private static Map<Integer, Long> getRowCountZKNode(int nodeId){
		ZKNodeFile zkNodeFileId = null;
		while(zkNodeFileId == null){
			zkNodeFileId = ZKUtils.getConfigZKNodeFile("_confnode"+String.valueOf(nodeId));
		}
		Object obj = zkNodeFileId.getObj();
		Map<Integer, Long> jobIdMemoMap = (obj == null)? null : (Map<Integer, Long>) obj;
		return jobIdMemoMap;
	}
	
}
