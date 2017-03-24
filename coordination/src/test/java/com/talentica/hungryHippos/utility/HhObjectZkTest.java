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
 *//*
package com.talentica.hungryHippos.utility;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryhippos.config.zookeeper.ZookeeperConfig;

*//**
 * @author pooshans
 *
 *//*
public class HhObjectZkTest {

  private HhClassTest hhObject;
  private static final String basePath = "/home/sohanc/D_drive/HungryHippos_project/HungryHippos";
  private static final String zookeeprConfigFilePath =
      basePath + "/configuration-schema/src/main/resources/schema/zookeeper-config.xml";

  @Before
  public void setUp() {
    hhObject = new HhClassTest();
    char[] chars = new char[] {'a', 'z', 'p'};
    hhObject.setChars(chars);
    List<String> list = new ArrayList<>();
    list.add("hungry");
    list.add("hippos");
    list.add("project");
    list.add("!");
    hhObject.setList(list);
    Map<String, List<String>> keyValue = new HashMap<>();
    List<String> list1 = new ArrayList<>();
    list1.add("zk");
    list1.add("@$1^");
    list1.add("1243");

    keyValue.put("001B", list1);

    hhObject.setKeyValue(keyValue);
  }

  @Test
  public void testSaveHhObjectZk() {
    String nodePath = "/rootnode/hhobject";
    ZkUtils.saveObjectZkNode(nodePath, hhObject);
    HhClassTest object = (HhClassTest) ZkUtils.readObjectZkNode(nodePath);
    Assert.assertEquals(hhObject, object);
  }

  @Test
  @Ignore
  public void testClientConfigZk() {
    ZookeeperConfig configSave =
        NodesManagerContext.getZookeeperConfiguration(zookeeprConfigFilePath);
    ZkUtils.saveObjectZkNode("/rootnode/configuration2/A/B", configSave);
    ZookeeperConfig configRet =
        (ZookeeperConfig) ZkUtils.readObjectZkNode("/rootnode/configuration2/A/B");
    Assert.assertNotNull(configRet);
  }
}
*/