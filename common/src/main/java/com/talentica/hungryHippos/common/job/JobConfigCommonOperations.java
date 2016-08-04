package com.talentica.hungryHippos.common.job;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.utility.JobEntity;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class is for having common methods related to Job Configurations
 * Created by rajkishoreh on 3/8/16.
 */
public class JobConfigCommonOperations {

    public static final Logger LOGGER = LoggerFactory.getLogger(JobConfigCommonOperations.class);

    private static final String INPUT_HH_PATH = "INPUT_HH_PATH";
    private static final String OUTPUT_HH_PATH = "OUTPUT_HH_PATH";
    private static final String CLASS_NAME = "CLASS_NAME";
    private static final String JOB_LIST = "JOB_LIST";

    public static String getJobClassNode(String jobNode){
        return jobNode+"/"+ CLASS_NAME;
    }

    public static String getJobInputNode(String jobNode){
        return jobNode+"/"+INPUT_HH_PATH;
    }

    public static String getJobOutputNode(String jobNode){
        return jobNode+"/"+OUTPUT_HH_PATH;
    }

    public static String getJobListNode(String jobNode){
        return jobNode+"/"+ JOB_LIST;
    }

    /**
     * Returns jobEntity Node
     * @param jobNode
     * @param jobEntityId
     * @return
     */
    public static String getJobEntityIdNode(String jobNode, String jobEntityId){
        String jobListNode = getJobListNode(jobNode);
        return jobListNode+"/"+jobEntityId;
    }

    /**
     * Returns Job Node
     * @param jobUUID
     * @return
     */
    public static String getJobNode(String jobUUID){
        String jobConfigsRootNode = CoordinationApplicationContext.getZkCoordinationConfigCache().
                getZookeeperDefaultConfig().getJobConfigPath();
        return jobConfigsRootNode + "/" + jobUUID;
    }

    /**
     * Returns String data from config node
     * @param node
     * @return
     */
    public static String getConfigNodeData(String node){
        String configValue = "";
        try {
            NodesManager manager = NodesManagerContext.getNodesManagerInstance();
            configValue = (String) manager.getObjectFromZKNode(node);
        } catch (IOException | KeeperException | InterruptedException |JAXBException |ClassNotFoundException e) {
            LOGGER.error(e.toString());
            throw new RuntimeException(e);
        }
        return configValue;
    }

    /**
     * Returns Job Entity Object
     * @param node
     * @return
     */
    public static JobEntity getJobEntityObject(String node){
        JobEntity jobEntity = null;
        try {
            NodesManager manager = NodesManagerContext.getNodesManagerInstance();
            jobEntity = (JobEntity) manager.getObjectFromZKNode(node);
        } catch (IOException | KeeperException | InterruptedException |JAXBException |ClassNotFoundException e) {
            LOGGER.error(e.toString());
            throw new RuntimeException(e);
        }
        return jobEntity;
    }

    /**
     * Returns list of child nodes
     * @param parentNode
     * @return
     */
    public static List<String> getChildren(String parentNode){
        List<String> stringList = new ArrayList<>();
        try {
            NodesManager manager = NodesManagerContext.getNodesManagerInstance();
            stringList = manager.getChildren(parentNode);
        } catch (FileNotFoundException | JAXBException | KeeperException | InterruptedException e) {
            LOGGER.error(e.toString());
            throw new RuntimeException(e);
        }
        return  stringList;
    }



}
