package com.talentica.hungryHippos.master.job;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZKUtils;
import com.talentica.hungryHippos.coordination.domain.ServerHeartBeat;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
import com.talentica.hungryHippos.master.data.DataProvider;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.utility.CommonUtil;
import com.talentica.hungryHippos.utility.CommonUtil.ZKNodeDeleteSignal;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.ZKNodeName;

public class JobManager {

	private NodesManager nodesManager;
	private Map<String, Map<Object, Node>> keyValueNodeNumberMap;
	private static final Logger LOGGER = LoggerFactory.getLogger(JobManager.class);
	private List<Job> jobs = new ArrayList<Job>();
	
	public void addJobList(List<Job> jobList){
		this.jobs.addAll(jobList);
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
			
			LOGGER.info("SEND TASKS TO NODES");
			sendJobsToNodes();
			LOGGER.info("TASKS SENT TO NODES SUCCESSFULLY");
			
			LOGGER.info("\n\n\n\t*****SPAWNING THE JOBS ACROSS NODES****\n\n\n");
			executeJobsOnNodes();
			LOGGER.info("JOBS SENT TO NODES SUCCESSFULLY!");
			
			LOGGER.info("GET FINISHED SIGNAL FROM NODES");
			getFinishNodeJobsSignal();
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
			if(!getFinishSignal(nodeId,CommonUtil.ZKJobNodeEnum.FINISH_ROW_COUNT.name())){
				continue;
			}
			if(jobs == null || jobs.isEmpty()) continue;
			sendJobMatrixRunSignalToNodes(nodeId);
		}
		
		LOGGER.info("Time taken(Sec) in node create ::" + (new Date().getTime()-startTime)/1000);
		
	}
	
	
	
	/**
	 * Get finish jobs matrix signal.
	 */
	private void getFinishNodeJobsSignal(){
		Map<Integer,Node> nodeIdNodeMap = getNodeIdNodesMap();
		Iterator<Node> nodesItr = nodeIdNodeMap.values().iterator();
		while(nodesItr.hasNext()){
			Node node = nodesItr.next();
			if(!getFinishSignal(node.getNodeId(),CommonUtil.ZKJobNodeEnum.FINISH_JOB_MATRIX.name())){
				continue;
			}else{
				LOGGER.info("NODE ID {} FINISHED THE JOBS",node.getNodeId());
			}
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
	 * Wait for finish signal from node.
	 * 
	 * @param nodeId
	 * @param finishNode
	 * @return boolean
	 */
	private boolean getFinishSignal(Integer nodeId,String finishNode){
		CountDownLatch signal = new CountDownLatch(1);
		String buildPath = ZKUtils.buildNodePath(nodeId) + PathUtil.FORWARD_SLASH + finishNode;
		try {
			ZKUtils.waitForSignal(buildPath, signal);
			signal.await();
		} catch (KeeperException | InterruptedException e) {
			return false;
		}
		return true;
	}
	
	
	
	/**
	 * Push the jobs to ZK nodes for execution.
	 * 
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	private void sendJobsToNodes() throws ClassNotFoundException, IOException, InterruptedException, KeeperException{
		Map<Integer,Node> nodeIdNodeMap = getNodeIdNodesMap();
		for (Integer nodeId : nodeIdNodeMap.keySet()) {
			if(jobs == null || jobs.isEmpty()) continue;
			NodeJobsService nodeJobsService = new NodeJobsService(nodeIdNodeMap.get(nodeId),nodesManager);
			nodeJobsService.addJobs(jobs);
			nodeJobsService.createNodeJobService();
			nodeJobsService.scheduleTaskManager();
		}
	}
	
	
	
	/**
	 * Send the notification to nodes to start the jobs matrix executions.
	 * 
	 * @param nodeId
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void sendJobMatrixRunSignalToNodes(Integer nodeId) throws IOException, InterruptedException{
		String buildPath = ZKUtils.buildNodePath(nodeId) + PathUtil.FORWARD_SLASH + CommonUtil.ZKJobNodeEnum.START_JOB_MATRIX.name();
		nodesManager.createNode(buildPath,null);
	}
	
}
