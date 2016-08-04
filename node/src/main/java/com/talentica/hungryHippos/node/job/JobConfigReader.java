package com.talentica.hungryHippos.node.job;

import com.talentica.hungryHippos.utility.JobEntity;

import java.util.ArrayList;
import java.util.List;

import static com.talentica.hungryHippos.common.job.JobConfigCommonOperations.*;

/**
 * Created by rajkishoreh on 3/8/16.
 */
public class JobConfigReader {

    public static String readClassName(String jobUUID) {
        String jobNode = getJobNode(jobUUID);
        String jobClassNode = getJobClassNode(jobNode);
        return getConfigNodeData(jobClassNode);
    }

    public static String readInputPath(String jobUUID) {
        String jobNode = getJobNode(jobUUID);
        String jobInputNode = getJobInputNode(jobNode);
        return getConfigNodeData(jobInputNode);
    }

    public static String readOutputPath(String jobUUID) {
        String jobNode = getJobNode(jobUUID);
        String jobOutputNode = getJobOutputNode(jobNode);
        return getConfigNodeData(jobOutputNode);
    }

    public static List<JobEntity> getJobEntityList(String jobUUID){
        List<JobEntity> jobEntityList = new ArrayList<>();
        String jobListNode=  getJobListNode(jobUUID);
        List<String> jobIdList = getChildren(jobListNode);
        for(String jobId: jobIdList){
            JobEntity jobEntity = getJobEntity(jobUUID,jobId);
            jobEntityList.add(jobEntity);
        }
        return jobEntityList;
    }

    public static JobEntity getJobEntity(String jobUUID, String jobId){
        String jobIdNode=  getJobEntityIdNode(jobUUID,jobId);
        JobEntity jobEntity = getJobEntityObject(jobIdNode);
        return jobEntity;
    }
}
