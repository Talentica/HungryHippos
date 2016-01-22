package com.talentica.hungryHippos.master.job;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.common.JobEntity;
import com.talentica.hungryHippos.common.JobRunner;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZKUtils;
import com.talentica.hungryHippos.coordination.domain.ServerHeartBeat;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
import com.talentica.hungryHippos.master.data.DataProvider;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryHippos.storage.NodeDataStoreIdCalculator;
import com.talentica.hungryHippos.utility.CommonUtil;
import com.talentica.hungryHippos.utility.CommonUtil.ZKNodeDeleteSignal;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.ZKNodeName;
import com.talentica.hungryHippos.utility.marshaling.FieldTypeArrayDataDescription;

public class JobManager {

	//private Set<Server> regServer = null;
	//static String configPath;
	NodesManager nodesManager;
	private Map<String, Map<Object, Node>> keyValueNodeNumberMap;
	private static final Logger LOGGER = LoggerFactory.getLogger(JobManager.class);
	private List<Job> jobList = new ArrayList<Job>();
	private List<JobEntity> jobEntities = new ArrayList<JobEntity>();
	
	private Map<Integer,Map<Integer,JobEntity>> nodeJobIdRowCountMap = new HashMap<Integer, Map<Integer,JobEntity>>();
	
	public Map<Integer,Map<Integer,JobEntity>> getNodeJobIdRowCountMap(){
		return this.nodeJobIdRowCountMap;
	}
	
	public void addJobList(List<Job> jobList){
		this.jobList = jobList;
	}
	
	/**
	 * To start the job manager.
	 * 
	 * @throws Exception
	 */
	public void start() throws Exception{
		
			LOGGER.info("START NODE MANAGER..");
			(nodesManager = ServerHeartBeat.init()).startup(ZKNodeDeleteSignal.MASTER.name());
			LOGGER.info("NODE MANAGER STARTED SUCCESSFULLY!");
			
			LOGGER.info("PUT THE CONFIG FILE TO ZK NODE");
			ZKNodeFile serverConfigFile = new ZKNodeFile(Property.SERVER_CONF_FILE,Property.loadServerProperties());
			nodesManager.saveConfigFileToZNode(serverConfigFile,null);
			LOGGER.info("CONFIG FILE PUT TO ZK NODE SUCCESSFULLY!");
			
			ZKNodeFile configNodeFile = new ZKNodeFile(Property.CONF_PROP_FILE+"_FILE",Property.getProperties());
			nodesManager.saveConfigFileToZNode(configNodeFile,null);
			
		    LOGGER.info("PUT keyValueNodeNumberMap TO ZK NODE");
			setKeyValueNodeNumberMap();
			
			LOGGER.info("PUBLISH DATA ACROSS THE NODES");
			DataProvider.publishDataToNodes(nodesManager);
			LOGGER.info("DATA PUBLISHED SUCCESSFULLY!");
			
			LOGGER.info("SENDING JOBRUNNER TO NODES TO CALCULATE ROWCOUNT/JOB");
			createAndSendJobRunnerZKNode();
			LOGGER.info("JOB RUNNER SENT SUCESSFULLY!");
			
			LOGGER.info("\n\n\n\t*****SPAWNING THE JOBS ACROSS NODES****\n\n\n");
			executeJobsOnNodes();
			LOGGER.info("JOBS SENT TO NODES SUCCESSFULLY!");
			
			LOGGER.info("\n\n\n\tMEMORY FOOTPRINT\n\n\n");
			outMemoryFootprint();
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
			List<JobEntity> jobEntities = getJobIdJobEntity(nodeId);
			if(jobEntities == null || jobEntities.isEmpty()) continue;
			NodeJobsService nodeJobsService = new NodeJobsService(nodeIdNodeMap.get(nodeId),nodesManager);
			this.jobEntities.addAll(jobEntities);
			for(JobEntity jobEntity : jobEntities){
				nodeJobsService.addJobEntity(jobEntity);
			}
			nodeJobsService.createNodeJobService();
			nodeJobsService.scheduleTaskManager();
		}
		
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
		FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription();
        CommonUtil.setDataDescription(dataDescription);
		dataDescription.setKeyOrder(Property.getKeyOrder());
		for (int nodeId = 0; nodeId < Property.getTotalNumberOfNodes(); nodeId++) {
			NodeDataStoreIdCalculator nodeDataStoreIdCalculator = new NodeDataStoreIdCalculator(
					keyValueNodeNumberMap, nodeId, dataDescription);
			int totalDimensions = Property.getKeyOrder().length;
			FileDataStore dataStore = new FileDataStore(totalDimensions,
					nodeDataStoreIdCalculator, dataDescription, true);
			JobRunner jobRunner = new JobRunner(dataDescription, dataStore);

			putJobToRunner(jobRunner);

			sendJobsConfiguration(jobRunner, nodeId);

		}
	}
	
	/**
	 * Get NodeId and Node Map.
	 * 
	 * @return Map<Integer,Node>
	 */
	private Map<Integer,Node> getNodeIdNodesMap(){
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
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	private List<JobEntity> getJobIdJobEntity(int nodeId) throws KeeperException, InterruptedException, ClassNotFoundException, IOException{
		CountDownLatch signal = new CountDownLatch(1);
		String nodePath = String.valueOf(nodesManager.buildConfigPath("_node" + nodeId) + PathUtil.FORWARD_SLASH + "FINISH");		
		ZKUtils.isNodeExists(nodePath, signal);
		signal.await();
		List<JobEntity> jobEntites = new ArrayList<JobEntity>();
		for(Job job : jobList){
			String jobPath = ("_node" + nodeId) + PathUtil.FORWARD_SLASH + ("_job" + job.getJobId());
			ZKNodeFile zkNodeJob = ZKUtils.getConfigZKNodeFile(jobPath);
			if(zkNodeJob != null && zkNodeJob.getObj() != null) {
			 JobEntity jobEntity = (JobEntity) zkNodeJob.getObj();
			 jobEntites.add(jobEntity);
			}
		}
		return jobEntites;
	}
	
	
	
	/**
	 * Get the keyValueNodeNumberMap from local storage.
	 * 
	 * @throws Exception
	 * 
	 */
	@SuppressWarnings("unchecked")
	private void setKeyValueNodeNumberMap() throws Exception {
		try (ObjectInputStream inKeyValueNodeNumberMap = new ObjectInputStream(
				new FileInputStream(
						new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath()
								+ PathUtil.FORWARD_SLASH
								+ ZKNodeName.keyValueNodeNumberMap))) {
			this.keyValueNodeNumberMap = (Map<String, Map<Object, Node>>) inKeyValueNodeNumberMap.readObject();
		} catch (IOException | ClassNotFoundException exception) {
			LOGGER.info("Unable to read file");
			throw exception;
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
			nodesManager.saveConfigFileToZNode(jobConfigFile,null);
		}
	
	private void outMemoryFootprint(){
		for(JobEntity jobEntity : this.jobEntities){
			LOGGER.info("Size of the JobEntity {}",jobEntities.size());
			for(Entry<String,Integer> workerIdRowCount : jobEntity.getWorkerIdRowCountMap().entrySet()){
				long memoryFootprint = jobEntity.getJob().getMemoryFootprint(workerIdRowCount.getValue());
				LOGGER.info("JOBID {} AND WORKER[ID_KEYVALUE] {} AND ROW COUNT {} AND MEMORY FOOTPRINT {}",jobEntity.getJob().getJobId(),workerIdRowCount.getKey(),workerIdRowCount.getValue(),memoryFootprint);
			}
		}
	}
}
