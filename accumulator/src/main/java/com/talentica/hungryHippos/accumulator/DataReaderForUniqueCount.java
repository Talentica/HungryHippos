package com.talentica.hungryHippos.accumulator;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.accumulator.testJobs.TestJobUniqueCount;
import com.talentica.hungryHippos.node.NodeInitializer;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryHippos.storage.NodeDataStoreIdCalculator;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryHippos.utility.marshaling.DataLocator;
import com.talentica.hungryHippos.utility.marshaling.FieldTypeArrayDataDescription;

/**
 * Created by debasishc on 12/10/15.
 */
public class DataReaderForUniqueCount {

	private static Logger LOGGER = LoggerFactory.getLogger(DataReaderForUniqueCount.class);

    private static Map<String,Map<Object, Node>> keyValueNodeNumberMap ;


    public static void main(String [] args) throws Exception {
        FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription();
        dataDescription.addFieldType(DataLocator.DataType.STRING,2);
        dataDescription.addFieldType(DataLocator.DataType.STRING,2);
        dataDescription.addFieldType(DataLocator.DataType.STRING,2);
        dataDescription.addFieldType(DataLocator.DataType.STRING,2);
        dataDescription.addFieldType(DataLocator.DataType.STRING,2);
        dataDescription.addFieldType(DataLocator.DataType.STRING,2);
        dataDescription.addFieldType(DataLocator.DataType.STRING,6);
        dataDescription.addFieldType(DataLocator.DataType.STRING,6);
        dataDescription.addFieldType(DataLocator.DataType.STRING,2);

        dataDescription.setKeyOrder(new String[]{"key1","key2","key3"});

        try(ObjectInputStream in
                    = new ObjectInputStream(new FileInputStream(new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath()+PathUtil.FORWARD_SLASH+"keyValueNodeNumberMap"))){
            keyValueNodeNumberMap = (Map<String, Map<Object, Node>>) in.readObject();
        } catch (Exception e) {
			LOGGER.error("Error occurred while reading keyValueNodeNumberMap file.", e);
        }

        NodeDataStoreIdCalculator nodeDataStoreIdCalculator
                = new NodeDataStoreIdCalculator(keyValueNodeNumberMap, NodeInitializer.readNodeId(),dataDescription);
        FileDataStore dataStore = new FileDataStore(3, nodeDataStoreIdCalculator, dataDescription, true);
        JobRunner jobRunner = new JobRunner(dataDescription, dataStore);
        int numMetrix = 0;
        for(int i=0;i<3;i++){
            jobRunner.addJob(new TestJobUniqueCount(new int[]{i}, i, 6));
            jobRunner.addJob(new TestJobUniqueCount(new int[]{i}, i, 7));
            numMetrix+=2;
            for(int j=i+1;j<5;j++){
                jobRunner.addJob(new TestJobUniqueCount(new int[]{i,j}, i, 6));
                jobRunner.addJob(new TestJobUniqueCount(new int[]{i,j}, j, 7));
                numMetrix+=2;
                for(int k=j+1;k<5;k++){
                    jobRunner.addJob(new TestJobUniqueCount(new int[]{i,j,k}, i, 6));
                    jobRunner.addJob(new TestJobUniqueCount(new int[]{i,j,k}, j, 7));
                    numMetrix+=2;
                }
            }
        }
        System.out.println(numMetrix);
        //jobRunner.run();

    }
}
