/**
 * 
 */
package com.talentica.hungryHippos.utility;

import java.io.IOException;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.talentica.hungryHippos.coordination.utility.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.utility.Property;
import com.talentica.hungryHippos.coordination.utility.ZkProperty;

/**
 * @author nitink
 *
 */
public class PropertyTest {

  @Test
  @Ignore
  public void testGetPropertyValueForMaster() {
    Object cleanupZookeeperNodesPropValue = CoordinationApplicationContext.getZkProperty().getValueByKey("cleanup.zookeeper.nodes");
    Assert.assertNotNull(cleanupZookeeperNodesPropValue);
    Assert.assertEquals("N", cleanupZookeeperNodesPropValue);
  }

  @Test
  @Ignore
  public void testGetPropertyValueForNode() {
    Object cleanupZookeeperNodesPropValue = CoordinationApplicationContext.getZkProperty().getValueByKey("cleanup.zookeeper.nodes");
    Assert.assertNotNull(cleanupZookeeperNodesPropValue);
    Assert.assertEquals("N", cleanupZookeeperNodesPropValue);
  }

  @Test
  @Ignore
  public void testGetPropertyValueWithoutNamespace() {
    Object zookeeperServerIps = CoordinationApplicationContext.getZkProperty().getValueByKey("zookeeper.server.ips");
    Assert.assertNotNull(zookeeperServerIps);
  }

  @Test
  @Ignore
  public void testGetKeyOrder() {
    String[] keyOrder = CoordinationApplicationContext.getShardingDimensions();
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
    String[] keyNames = CoordinationApplicationContext.getKeyNamesFromIndexes(new int[] {1, 2});
    Assert.assertNotNull(keyNames);
    Assert.assertEquals(2, keyNames.length);
    Assert.assertEquals("key2", keyNames[0]);
    Assert.assertEquals("key3", keyNames[1]);
  }

  @Test
  @Ignore
  public void testGetKeyNamesFromIndexesWithEmptyArray() {
    String[] keyNames = CoordinationApplicationContext.getKeyNamesFromIndexes(new int[] {});
    Assert.assertNotNull(keyNames);
    Assert.assertEquals(0, keyNames.length);
  }

  @Test
  @Ignore
  public void testGetEnvironmentSpecificProperty() {
    Assert.assertEquals(2, CoordinationApplicationContext.getShardingDimensions().length);
  }

  @Test
  @Ignore
  public void testGetShardingIndexes() {
    int[] shardingKeyIndexes = CoordinationApplicationContext.getShardingIndexes();
    Assert.assertNotNull(shardingKeyIndexes);
    Assert.assertEquals(2, shardingKeyIndexes.length);
    Assert.assertEquals(2, shardingKeyIndexes[0]);
    Assert.assertEquals(3, shardingKeyIndexes[1]);
  }

  @Test
  @Ignore
  public void testGetShardingIndexSequence() {
    Assert.assertEquals(1, CoordinationApplicationContext.getShardingIndexSequence(3));

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
