package com.talentica.hungryHippos.sharding;



import java.io.FileNotFoundException;
import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.junit.Test;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;


public class KeyValueFrequencyIntegrationTest {

  @Test
  public void testEquals() throws ClassNotFoundException, FileNotFoundException, KeeperException,
      InterruptedException, IOException, JAXBException {
    FieldTypeArrayDataDescription dataDescription =
        FieldTypeArrayDataDescription.createDataDescription(new String[0], 1);
    dataDescription.setKeyOrder(ShardingApplicationContext.getShardingDimensions());
    MutableCharArrayString mutableCharArrayStringL1 = new MutableCharArrayString(5);
    KeyValueFrequency keyValue1frequency1 = new KeyValueFrequency(mutableCharArrayStringL1, 10);
    MutableCharArrayString mutableCharArrayStringL2 = new MutableCharArrayString(5);
    KeyValueFrequency keyValue1frequency2 = new KeyValueFrequency(mutableCharArrayStringL2, 10);
    Assert.assertEquals(keyValue1frequency1.hashCode(), keyValue1frequency2.hashCode());
    Assert.assertTrue(keyValue1frequency1.equals(keyValue1frequency2));
    Assert.assertTrue(keyValue1frequency2.equals(keyValue1frequency1));
  }

}
