package com.talentica.hungryHippos.node.job;

import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.node.JobExecutor;
import com.talentica.hungryHippos.utility.JobEntity;

import java.util.ArrayList;
import java.util.List;

import static com.talentica.hungryHippos.common.job.JobConfigCommonOperations.*;

/**
 * This class is for Nodes to fetch the configurations for the job
 * Created by rajkishoreh on 3/8/16.
 */
public class JobConfigReader {

    /**
     * Returns Class Name
     * @param jobUUID
     * @return
     */
    public static String readClassName(String jobUUID) {
        String jobNode = getJobNode(jobUUID);
        String jobClassNode = getJobClassNode(jobNode);
        return getConfigNodeData(jobClassNode);
    }

    /**
     * Returns Input Path for job
     * @param jobUUID
     * @return
     */
    public static String readInputPath(String jobUUID) {
        String jobNode = getJobNode(jobUUID);
        String jobInputNode = getJobInputNode(jobNode);
        return getConfigNodeData(jobInputNode);
    }

    /**
     * Returns Output Path for job
     * @param jobUUID
     * @return
     */
    public static String readOutputPath(String jobUUID) {
        String jobNode = getJobNode(jobUUID);
        String jobOutputNode = getJobOutputNode(jobNode);
        return getConfigNodeData(jobOutputNode);
    }

}
