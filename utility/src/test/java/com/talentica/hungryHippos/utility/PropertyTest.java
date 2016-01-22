/**
 * 
 */
package com.talentica.hungryHippos.utility;

import org.junit.Assert;
import org.junit.Test;

import com.talentica.hungryHippos.utility.Property.PROPERTIES_NAMESPACE;

/**
 * @author nitink
 *
 */
public class PropertyTest{
	
	@Test
	public void testGetPropertyValueForMaster() {
		Property.setNamespace(PROPERTIES_NAMESPACE.MASTER);
		Object cleanupZookeeperNodesPropValue = Property.getPropertyValue("cleanup.zookeeper.nodes");
		Assert.assertNotNull(cleanupZookeeperNodesPropValue);
		Assert.assertEquals("Y", cleanupZookeeperNodesPropValue);
	}

	@Test
	public void testGetPropertyValueForNode() {
		Property.setNamespace(PROPERTIES_NAMESPACE.NODE);
		Object cleanupZookeeperNodesPropValue = Property.getPropertyValue("cleanup.zookeeper.nodes");
		Assert.assertNotNull(cleanupZookeeperNodesPropValue);
		Assert.assertEquals("N", cleanupZookeeperNodesPropValue);
	}

	@Test
	public void testGetPropertyValueWithoutNamespace() {
		Property.setNamespace(null);
		Object zookeeperServerIps = Property.getPropertyValue("zookeeper.server.ips");
		Assert.assertNotNull(zookeeperServerIps);
	}

	@Test
	public void testGetKeyOrder() {
		Property.setNamespace(null);
		String[] keyOrder= Property.getKeyOrder();
		Assert.assertNotNull(keyOrder);
		for(String key:keyOrder){
			Assert.assertNotNull(key);
			Assert.assertFalse("".equals(key.trim()));
			Assert.assertFalse(key.contains(","));
		}
	}

}
