package com.talentica.hungryHippos.accumulator;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.Map;

import com.talentica.hungryHippos.accumulator.testJobs.TestJob;
import com.talentica.hungryHippos.node.NodeInitializer;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryHippos.storage.NodeDataStoreIdCalculator;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryHippos.utility.marshaling.DataLocator;
import com.talentica.hungryHippos.utility.marshaling.FieldTypeArrayDataDescription;

/**
 * Created by debasishc on 9/9/15.
 */
public class DataReader {
    private static Map<String,Map<Object, Node>> keyValueNodeNumberMap ;


    public static void main(String [] args) throws Exception {
        FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription();
        dataDescription.addFieldType(DataLocator.DataType.STRING,2);
        dataDescription.addFieldType(DataLocator.DataType.STRING,2);
        dataDescription.addFieldType(DataLocator.DataType.STRING,2);
        dataDescription.addFieldType(DataLocator.DataType.STRING,4);
        dataDescription.addFieldType(DataLocator.DataType.STRING,4);
        dataDescription.addFieldType(DataLocator.DataType.STRING,4);
        dataDescription.addFieldType(DataLocator.DataType.DOUBLE,0);
        dataDescription.addFieldType(DataLocator.DataType.DOUBLE, 0);
        dataDescription.addFieldType(DataLocator.DataType.STRING, 4);

        dataDescription.setKeyOrder(new String[]{"key1","key2","key3"});

        try(ObjectInputStream in
                    = new ObjectInputStream(new FileInputStream(new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath()+PathUtil.FORWARD_SLASH+"keyValueNodeNumberMap"))){
            keyValueNodeNumberMap = (Map<String, Map<Object, Node>>) in.readObject();
           // System.out.println(keyValueNodeNumberMap);
        } catch (Exception e) {
            e.printStackTrace();
        }


        NodeDataStoreIdCalculator nodeDataStoreIdCalculator
                = new NodeDataStoreIdCalculator(keyValueNodeNumberMap, NodeInitializer.readNodeId(),dataDescription);
        FileDataStore dataStore = new FileDataStore(3, nodeDataStoreIdCalculator, dataDescription, true);

        JobRunner jobRunner = new JobRunner(dataDescription, dataStore, keyValueNodeNumberMap);
        
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
        //jobRunner.run();

    }
}
