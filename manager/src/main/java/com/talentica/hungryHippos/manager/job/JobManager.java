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

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.accumulator.DataProvider;
import com.talentica.hungryHippos.accumulator.Job;
import com.talentica.hungryHippos.accumulator.JobEntity;
import com.talentica.hungryHippos.accumulator.JobRunner;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryHippos.storage.NodeDataStoreIdCalculator;
import com.talentica.hungryHippos.utility.CommonUtil;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.ZKNodeName;
import com.talentica.hungryHippos.utility.marshaling.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.utility.zookeeper.ServerHeartBeat;
import com.talentica.hungryHippos.utility.zookeeper.ZKNodeFile;
import com.talentica.hungryHippos.utility.zookeeper.ZKUtils;
import com.talentica.hungryHippos.utility.zookeeper.manager.NodesManager;

public class JobManager {

	//private Set<Server> regServer = null;
	//static String configPath;
	NodesManager nodesManager;
	private Map<String, Map<Object, Node>> keyValueNodeNumberMap;
	private static final Logger LOGGER = LoggerFactory.getLogger(JobManager.class.getName());
	private List<Job> jobList = new ArrayList<Job>();
	
	public void addJobList(List<Job> jobList){
		this.jobList = jobList;
	}
	
	public static void main(String[] args) throws Exception {
		//please do not delete this comment. Need to use further.
		
		
		/*if (args.length == 0) {
			LOGGER.info("You have not provided external config.properties file. Default config.properties file will be use internally");
		} else if (args.length == 1) {
			Property.CONFIG_FILE = new FileInputStream(new String(args[0]));
		}*/
		
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
	
	public void start() throws Exception{
		
			LOGGER.info("START NODE MANAGER..");
			(nodesManager = ServerHeartBeat.init()).startup();
			LOGGER.info("NODE MANAGER STARTED SUCCESSFULLY!");
			
			LOGGER.info("PUT THE CONFIG FILE TO ZK NODE");
			ZKNodeFile serverConfigFile = new ZKNodeFile(Property.SERVER_CONF_FILE,Property.loadServerProperties());
			nodesManager.saveConfigFileToZNode(serverConfigFile);
			LOGGER.info("CONFIG FILE PUT TO ZK NODE SUCCESSFULLY!");
			
		    LOGGER.info("PUT keyValueNodeNumberMap TO ZK NODE");
			setKeyValueNodeNumberMap();
			ZKNodeFile saveKeyValueNodeNumberMap = new ZKNodeFile(ZKNodeName.keyValueNodeNumberMap, null,keyValueNodeNumberMap);
			nodesManager.saveConfigFileToZNode(saveKeyValueNodeNumberMap);
			LOGGER.info("keyValueNodeNumberMap PUT TO ZK NODE SUCCESSFULLY!");
			
			LOGGER.info("PUBLISH DATA ACROSS THE NODES");
			DataProvider.publishDataToNodes(nodesManager);
			LOGGER.info("DATA PUBLISHED SUCCESSFULLY!");
			
			LOGGER.info("SENDING JOBRUNNER TO NODES TO CALCULATE ROWCOUNT/JOB");
			createAndSendJobRunnerZKNode();
			LOGGER.info("JOB RUNNER SENT SUCESSFULLY!");
			
			LOGGER.info("\n\n\n\t*****SPAWNING THE JOBS ACROSS NODES****\n\n\n");
			executeJobsOnNodes();
			LOGGER.info("\n\n\n\t FINISHED!\n\n\n");
			
		
	}
	
	
	/**
	 * It will schedule the task manager for each NODES though ZK.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws KeeperException
	 * @throws ClassNotFoundException
	 */
	private void executeJobsOnNodes() throws IOException, InterruptedException, KeeperException, ClassNotFoundException{
		long startTime = new Date().getTime();
		Map<Integer,Node> nodeIdNodeMap = getNodeIdNodesMap();
		LOGGER.info("No. of nodes :: " + nodeIdNodeMap.size());
		for (Integer nodeId : nodeIdNodeMap.keySet()) {
			Map<Integer,JobEntity> jobIdJobEntityMap = getJobIdJobEntity(nodeId);
			NodeJobsService nodeJobsService = new NodeJobsService(nodeIdNodeMap.get(nodeId),nodesManager);
			for (Integer jobIdKey : jobIdJobEntityMap.keySet()) {
				JobEntity jobRowCount = jobIdJobEntityMap.get(jobIdKey);
				nodeJobsService.addJobEntity(jobRowCount);
			}
			nodeJobsService.createNodeJobService();
			nodeJobsService.scheduleTaskManager();
		}
		
		LOGGER.info("ALL JOBS ARE FINISHED!");
		LOGGER.info("Time taken(Sec) in node create ::" + (new Date().getTime()-startTime)/1000);
		
	}
	
	/**
	 * Send the JobRunner to ZK Nodes.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	private void createAndSendJobRunnerZKNode() throws IOException, InterruptedException, KeeperException{
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

			putJobToRunner(jobRunner);

			sendJobsConfiguration(jobRunner, nodeId);

		}
	}
	
	
	
	/**
	 * Get NodeId and Node Map.
	 * 
	 * @return Map<Integer,Node>
	 */
	@SuppressWarnings("unchecked")
	private Map<Integer,Node> getNodeIdNodesMap(){
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
	
	
	
	/**
	 * Get JobIdJobEntity Map from ZK NODE which consist of the row counts for each jobs from each NODES.
	 * 
	 * @param nodeId
	 * @return Map<Integer,JobEntity>
	 */
	@SuppressWarnings("unchecked")
	private Map<Integer,JobEntity> getJobIdJobEntity(int nodeId){
		ZKNodeFile zkNodeFileId = null;
		while(zkNodeFileId == null){
			zkNodeFileId = ZKUtils.getConfigZKNodeFile("_confnode"+String.valueOf(nodeId));
		}
		Object obj = zkNodeFileId.getObj();
		Map<Integer,JobEntity> jobIdJobEntityMap = (obj == null)? null : (Map<Integer,JobEntity>) obj;
		return jobIdJobEntityMap;
	}
	
	
	
	/**
	 * Get the keyValueNodeNumberMap from local storage.
	 * 
	 */
	@SuppressWarnings("unchecked")
	private void setKeyValueNodeNumberMap(){
		try (ObjectInputStream inKeyValueNodeNumberMap = new ObjectInputStream(
				new FileInputStream(
						new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath()
								+ PathUtil.FORWARD_SLASH
								+ ZKNodeName.keyValueNodeNumberMap))) {
			this.keyValueNodeNumberMap = (Map<String, Map<Object, Node>>) inKeyValueNodeNumberMap.readObject();
		} catch (IOException|ClassNotFoundException e) {
			LOGGER.info("Unable to read file");
		}
	}
	
	
	
	/**
	 * Populate the JobRunner with the list of jobs which will further be used to run JobRunner on each nodes.
	 * 
	 * @param jobRunner
	 * @throws IOException
	 */
	private void putJobToRunner(JobRunner jobRunner) throws IOException{
		  for(Job job : jobList){
			  jobRunner.addJob(job);
		  }
			
		}
		
	
	
	/**
	 * Send the JobRunner instances to the NODES through ZK Nodes.
	 * 
	 * @param jobRunner
	 * @param nodeId
	 * @throws IOException
	 */
	private void sendJobsConfiguration(JobRunner jobRunner,int nodeId) throws IOException{
			ZKNodeFile jobConfigFile = new ZKNodeFile("_node"+nodeId, null, new Object[]{jobRunner});
			nodesManager.saveConfigFileToZNode(jobConfigFile);
		}
	
}
