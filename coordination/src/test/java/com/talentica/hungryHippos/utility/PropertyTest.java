/**
 * 
 */
package com.talentica.hungryHippos.utility;

import java.io.IOException;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.talentica.hungryHippos.coordination.utility.ENVIRONMENT;
import com.talentica.hungryHippos.coordination.utility.Property;
import com.talentica.hungryHippos.coordination.utility.PropertyOld;
import com.talentica.hungryHippos.coordination.utility.PropertyOld.PROPERTIES_NAMESPACE;
import com.talentica.hungryHippos.coordination.utility.ZkProperty;

/**
 * @author nitink
 *
 */
public class PropertyTest {

  @Test
  @Ignore
  public void testGetPropertyValueForMaster() {
    PropertyOld.initialize(PROPERTIES_NAMESPACE.MASTER);
    Object cleanupZookeeperNodesPropValue = PropertyOld.getPropertyValue("cleanup.zookeeper.nodes");
    Assert.assertNotNull(cleanupZookeeperNodesPropValue);
    Assert.assertEquals("N", cleanupZookeeperNodesPropValue);
  }

  @Test
  @Ignore
  public void testGetPropertyValueForNode() {
    PropertyOld.initialize(PROPERTIES_NAMESPACE.NODE);
    Object cleanupZookeeperNodesPropValue = PropertyOld.getPropertyValue("cleanup.zookeeper.nodes");
    Assert.assertNotNull(cleanupZookeeperNodesPropValue);
    Assert.assertEquals("N", cleanupZookeeperNodesPropValue);
  }

  @Test
  @Ignore
  public void testGetPropertyValueWithoutNamespace() {
    PropertyOld.initialize(null);
    Object zookeeperServerIps = PropertyOld.getPropertyValue("zookeeper.server.ips");
    Assert.assertNotNull(zookeeperServerIps);
  }

  @Test
  @Ignore
  public void testGetKeyOrder() {
    PropertyOld.initialize(null);
    String[] keyOrder = PropertyOld.getShardingDimensions();
    Assert.assertNotNull(keyOrder);
    for (String key : keyOrder) {
      Assert.assertNotNull(key);
      Assert.assertFalse("".equals(key.trim()));
      Assert.assertFalse(key.contains(","));
    }
  }

  @Test
  @Ignore
  public void testGetKeyNamesFromIndexes() {
    String[] keyNames = PropertyOld.getKeyNamesFromIndexes(new int[] {1, 2});
    Assert.assertNotNull(keyNames);
    Assert.assertEquals(2, keyNames.length);
    Assert.assertEquals("key2", keyNames[0]);
    Assert.assertEquals("key3", keyNames[1]);
  }

  @Test
  @Ignore
  public void testGetKeyNamesFromIndexesWithEmptyArray() {
    String[] keyNames = PropertyOld.getKeyNamesFromIndexes(new int[] {});
    Assert.assertNotNull(keyNames);
    Assert.assertEquals(0, keyNames.length);
  }

  @Test
  @Ignore
  public void testGetEnvironmentSpecificProperty() {
    ENVIRONMENT.setCurrentEnvironment("LOCAL");
    Assert.assertEquals(2, PropertyOld.getShardingDimensions().length);
  }

  @Test
  @Ignore
  public void testGetShardingIndexes() {
    PropertyOld.initialize(PROPERTIES_NAMESPACE.MASTER);
    int[] shardingKeyIndexes = PropertyOld.getShardingIndexes();
    Assert.assertNotNull(shardingKeyIndexes);
    Assert.assertEquals(2, shardingKeyIndexes.length);
    Assert.assertEquals(2, shardingKeyIndexes[0]);
    Assert.assertEquals(3, shardingKeyIndexes[1]);
  }

  @Test
  @Ignore
  public void testGetShardingIndexSequence() {
    Assert.assertEquals(1, PropertyOld.getShardingIndexSequence(3));

  }

  @Test
  public void testPropertyByOverriding() throws IOException {
    Property<ZkProperty> property =
        new ZkProperty()
            .overrideProperty("/home/pooshans/HungryHippos/coordination/src/main/resources/zookeeper.properties");
    Properties properties = property.getProperties();
    Assert.assertNotNull(properties);
  }

  @Test
  public void testPropertyByConstructor() throws IOException {
    Property<ZkProperty> property = new ZkProperty("zookeeper.properties");
    Properties properties = property.getProperties();
    Assert.assertNotNull(properties);
  }

  @Test
  public void testPropertyBySetproperty() throws IOException {
    Property<ZkProperty> property = new ZkProperty().setPropertyFileName("zookeeper.properties");
    Properties properties = property.getProperties();
    Assert.assertNotNull(properties);
  }

}
