/**
 * 
 */
package com.talentica.hungryHippos.utility;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryhippos.config.coordination.CoordinationConfig;

/**
 * @author pooshans
 *
 */
public class ZkUtilsIntegrationTest {

	private HhClassTest hhObject;
	private static final String basePath = "/home/pooshans/HungryHippos";
	private static final String zookeeprConfigFilePath = basePath
			+ "/configuration-schema/src/main/resources/schema/zookeeper-config.xml";

	@Before
	public void setUp() throws Exception {
		NodesManagerContext.getNodesManagerInstance();
		hhObject = new HhClassTest();
		char[] chars = new char[] { 'a', 'z', 'p' };
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
	public void testSearchNodeByName() throws KeeperException, InterruptedException {
		List<String> nodePaths = new ArrayList<>();
		ZkUtils.getNodePathByName("/", "PUSH_JOB_NOTIFICATION", nodePaths);
		Assert.assertNotEquals(nodePaths.size(), 0);
	}

	@Test
	public void testSaveHhObjectZk() {
		String nodePath = "/rootnode/hhobject";
		ZkUtils.saveObjectZkNode(nodePath, hhObject);
		HhClassTest object = (HhClassTest) ZkUtils.readObjectZkNode(nodePath);
		Assert.assertEquals(hhObject, object);
	}

	@Test
	public void testClientConfigZk() {
    CoordinationConfig configSave = CoordinationApplicationContext.getZkCoordinationConfigCache();
		ZkUtils.saveObjectZkNode("/rootnode/configuration2/A/B", configSave);
    CoordinationConfig configRet =
        (CoordinationConfig) ZkUtils.readObjectZkNode("/rootnode/configuration2/A/B");
		Assert.assertNotNull(configRet);
	}

}
