/*package com.talentica.hungryHippos.storage;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;


public class NodeDataStoreIdCalculatorIntegrationTest {

  private NodeDataStoreIdCalculator nodeDataStoreIdCalculator;

  private int noOfBytesInOneDataSet;

  private Map<String, Map<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap = null;

  private Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap = null;

  @Before
  @SuppressWarnings("unchecked")
  public void setup() throws Exception {
    keyToValueToBucketMap =
        (Map<String, Map<Object, Bucket<KeyValueFrequency>>>) new ObjectInputStream(this.getClass()
            .getResourceAsStream("/keyToValueToBucketMap")).readObject();
    bucketToNodeNumberMap =
        (Map<String, Map<Bucket<KeyValueFrequency>, Node>>) new ObjectInputStream(this.getClass()
            .getResourceAsStream("/bucketToNodeNumberMap")).readObject();
    FieldTypeArrayDataDescription dataDescription = getDataDescription();
    noOfBytesInOneDataSet = dataDescription.getSize();
    nodeDataStoreIdCalculator =
        new NodeDataStoreIdCalculator(keyToValueToBucketMap, bucketToNodeNumberMap, 1,
            dataDescription);
  }

  @Test
  public void testStoreId() throws IOException {
    File dataFile =
        new File(this.getClass().getResource("/nodeDatatoreIdCalcSampleData.txt").getPath());
    FileInputStream fileInputStream = new FileInputStream(dataFile);
    DataInputStream dataInputStream = new DataInputStream(fileInputStream);
    try {
      long bytesRead = 0;
      while (dataInputStream.available() > 0) {
        byte[] bytes = new byte[noOfBytesInOneDataSet];
        bytesRead = bytesRead + noOfBytesInOneDataSet;
        dataInputStream.read(bytes);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int storeId = nodeDataStoreIdCalculator.storeId(buffer);
        Assert.assertTrue(storeId == 1 || storeId == 0);
      }
    } finally {
      fileInputStream.close();
    }
  }

  private FieldTypeArrayDataDescription getDataDescription() throws ClassNotFoundException, FileNotFoundException, KeeperException, InterruptedException, IOException, JAXBException {
    FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription(50);
    String[] datatypes =
        CoordinationApplicationContext.getZkCoordinationConfigCache().getInputFileConfig().getColumnDatatypeSize() .toString().split(",");
    for (String datatype : datatypes) {
      dataDescription.addFieldType(DataLocator.DataType.valueOf(datatype.split("-")[0]),
          Integer.valueOf(datatype.split("-")[1]));
    }
    dataDescription.setKeyOrder(CoordinationApplicationContext.getShardingDimensions());
    return dataDescription;
  }

}
*/