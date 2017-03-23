/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
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
    //dataDescription.setKeyOrder(ShardingApplicationContext.getShardingDimensions());
    MutableCharArrayString mutableCharArrayStringL1 = new MutableCharArrayString(5);
    KeyValueFrequency keyValue1frequency1 = new KeyValueFrequency(mutableCharArrayStringL1, 10);
    MutableCharArrayString mutableCharArrayStringL2 = new MutableCharArrayString(5);
    KeyValueFrequency keyValue1frequency2 = new KeyValueFrequency(mutableCharArrayStringL2, 10);
    Assert.assertEquals(keyValue1frequency1.hashCode(), keyValue1frequency2.hashCode());
    Assert.assertTrue(keyValue1frequency1.equals(keyValue1frequency2));
    Assert.assertTrue(keyValue1frequency2.equals(keyValue1frequency1));
  }

}
