/**
 * 
 */
package com.talentica.hungryHippos.utility;

import org.junit.Assert;
import org.junit.Test;

import com.talentica.hungryHippos.coordination.utility.ENVIRONMENT;
import com.talentica.hungryHippos.coordination.utility.Property;
import com.talentica.hungryHippos.coordination.utility.Property.PROPERTIES_NAMESPACE;

/**
 * @author nitink
 *
 */
public class PropertyTest {
	@Test
	public void testGetPropertyValueForMaster() {
		Property.initialize(PROPERTIES_NAMESPACE.MASTER);
		Object cleanupZookeeperNodesPropValue = Property.getPropertyValue("cleanup.zookeeper.nodes");
		Assert.assertNotNull(cleanupZookeeperNodesPropValue);
		Assert.assertEquals("N", cleanupZookeeperNodesPropValue);
	}

	@Test
	public void testGetPropertyValueForNode() {
		Property.initialize(PROPERTIES_NAMESPACE.NODE);
		Object cleanupZookeeperNodesPropValue = Property.getPropertyValue("cleanup.zookeeper.nodes");
		Assert.assertNotNull(cleanupZookeeperNodesPropValue);
		Assert.assertEquals("N", cleanupZookeeperNodesPropValue);
	}

	@Test
	public void testGetPropertyValueWithoutNamespace() {
		Property.initialize(null);
		Object zookeeperServerIps = Property.getPropertyValue("zookeeper.server.ips");
		Assert.assertNotNull(zookeeperServerIps);
	}

	@Test
	public void testGetKeyOrder() {
		Property.initialize(null);
		String[] keyOrder = Property.getShardingDimensions();
		Assert.assertNotNull(keyOrder);
		for (String key : keyOrder) {
			Assert.assertNotNull(key);
			Assert.assertFalse("".equals(key.trim()));
			Assert.assertFalse(key.contains(","));
		}
	}

	@Test
	public void testGetKeyNamesFromIndexes() {
		String[] keyNames = Property.getKeyNamesFromIndexes(new int[] { 1, 2 });
		Assert.assertNotNull(keyNames);
		Assert.assertEquals(2, keyNames.length);
		Assert.assertEquals("key2", keyNames[0]);
		Assert.assertEquals("key3", keyNames[1]);
	}

	@Test
	public void testGetKeyNamesFromIndexesWithEmptyArray() {
		String[] keyNames = Property.getKeyNamesFromIndexes(new int[] {});
		Assert.assertNotNull(keyNames);
		Assert.assertEquals(0, keyNames.length);
	}

	@Test
	public void testGetEnvironmentSpecificProperty() {
		ENVIRONMENT.setCurrentEnvironment("LOCAL");
		Assert.assertEquals(2, Property.getShardingDimensions().length);
	}

	@Test
	public void testGetShardingIndexes() {
		Property.initialize(PROPERTIES_NAMESPACE.MASTER);
		int[] shardingKeyIndexes = Property.getShardingIndexes();
		Assert.assertNotNull(shardingKeyIndexes);
		Assert.assertEquals(2, shardingKeyIndexes.length);
		Assert.assertEquals(2, shardingKeyIndexes[0]);
		Assert.assertEquals(3, shardingKeyIndexes[1]);
	}

	@Test
	public void testGetShardingIndexSequence() {
		Assert.assertEquals(1, Property.getShardingIndexSequence(3));

	}

}
