/**
 * 
 */
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

/**
 * @author pooshans
 *
 */
public class HhObjectZkTest {

  private HhClassTest hhObject;
  private static final String basePath = "/home/pooshans/HungryHippos";
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
  @Ignore
  public void testSaveHhObjectZk() {
    String nodePath = "/rootnode/hhobject";
    ZkUtils.saveObjectZkNode(nodePath, hhObject);
    HhClassTest object = (HhClassTest) ZkUtils.readObjectZkNode(nodePath);
    Assert.assertEquals(hhObject, object);
  }

  @Test
  public void testClientConfigZk() {
    ZookeeperConfig configSave =
        NodesManagerContext.getZookeeperConfiguration(zookeeprConfigFilePath);
    ZkUtils.saveObjectZkNode("/rootnode/configuration2/A/B", configSave);
    ZookeeperConfig configRet =
        (ZookeeperConfig) ZkUtils.readObjectZkNode("/rootnode/configuration2/A/B");
    Assert.assertNotNull(configRet);
  }
}
