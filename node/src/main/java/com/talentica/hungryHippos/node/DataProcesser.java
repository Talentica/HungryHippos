package com.talentica.hungryHippos.node;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.accumulator.JobRunner;
import com.talentica.hungryHippos.coordination.ZKUtils;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryHippos.storage.NodeDataStoreIdCalculator;
import com.talentica.hungryHippos.utility.ZKNodeName;
import com.talentica.hungryHippos.utility.marshaling.DataLocator;
import com.talentica.hungryHippos.utility.marshaling.FieldTypeArrayDataDescription;

/**
 * Created by debasishc on 9/9/15.
 */
public class DataProcesser {

	private static final Logger LOGGER = LoggerFactory.getLogger(DataProcesser.class);

	private static Map<String, Map<Object, Node>> keyValueNodeNumberMap;

    @SuppressWarnings("unchecked")
	public static JobRunner readData() throws Exception{
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
        ZKNodeFile zkNodeFile = ZKUtils.getConfigZKNodeFile(ZKNodeName.keyValueNodeNumberMap);
        keyValueNodeNumberMap = (Map<String, Map<Object, Node>>) zkNodeFile.getObj();
        NodeDataStoreIdCalculator nodeDataStoreIdCalculator
                = new NodeDataStoreIdCalculator(keyValueNodeNumberMap, NodeStarter.readNodeId(),dataDescription);
        FileDataStore dataStore = new FileDataStore(3, nodeDataStoreIdCalculator, dataDescription, true);
		JobRunner jobRunner = new JobRunner(dataDescription, dataStore);
        return jobRunner;
    }
}
