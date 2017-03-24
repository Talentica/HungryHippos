/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
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
/**
 * 
 */
package com.talentica.hungryHippos.node.test;

import java.io.IOException;
import java.util.Set;

import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.domain.LeafBean;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryhippos.config.client.CoordinationServers;
import com.talentica.hungryhippos.config.client.ObjectFactory;

/**
 * @author PooshanS
 *
 */
@Ignore
public class ConfigurationFileRetrieve {
  public final static String bucketToNodeNumberMapFile = "bucketToNodeNumberMap";
  public final static String bucketCombinationToNodeNumbersMapFile =
      "bucketCombinationToNodeNumbersMap";
  public final static String keyToValueToBucketMapFile = "keyToValueToBucketMap";

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ConfigurationFileRetrieve.class.getName());

  private HungryHippoCurator curator;

  @Before
  public void setUp() throws Exception {
    ObjectFactory factory = new ObjectFactory();
    CoordinationServers coordinationServers = factory.createCoordinationServers();
    // (curator = NodesManagerContext.getNodesManagerInstance()).startup(); //commented by sudarshan
  }

  @Test
  public void getKeyToValueToBucketMapFile() {
    String buildPath = curator.buildConfigPath(keyToValueToBucketMapFile);
    Set<LeafBean> leafs;
    try {
      leafs = curator.searchLeafNode(buildPath, null);
      for (LeafBean leaf : leafs) {
        LOGGER.info("Path is {} AND node name {}", leaf.getPath(), leaf.getName());
      }
    } catch (ClassNotFoundException | InterruptedException | KeeperException | IOException e) {
      LOGGER.info("Exception {}", e);
    } catch (HungryHippoException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

}
