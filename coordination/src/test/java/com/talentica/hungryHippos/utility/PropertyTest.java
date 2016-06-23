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

  public void testGetPropertyValueForMaster() {
    Object cleanupZookeeperNodesPropValue = CoordinationApplicationContext.getZkProperty().getValueByKey("cleanup.zookeeper.nodes");
    Assert.assertNotNull(cleanupZookeeperNodesPropValue);
    Assert.assertEquals("Y", cleanupZookeeperNodesPropValue);
  }

  @Test

  public void testGetPropertyValueForNode() {
    Object cleanupZookeeperNodesPropValue = CoordinationApplicationContext.getZkProperty().getValueByKey("cleanup.zookeeper.nodes");
    Assert.assertNotNull(cleanupZookeeperNodesPropValue);
    Assert.assertEquals("Y", cleanupZookeeperNodesPropValue);
  }

  @Test

  public void testGetPropertyValueWithoutNamespace() {
    Object zookeeperServerIps = CoordinationApplicationContext.getZkProperty().getValueByKey("zookeeper.server.ips");
    Assert.assertNotNull(zookeeperServerIps);
  }

  @Test

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

  public void testGetKeyNamesFromIndexes() {
    String[] keyNames = CoordinationApplicationContext.getKeyNamesFromIndexes(new int[] {1, 2});
    Assert.assertNotNull(keyNames);
    Assert.assertEquals(2, keyNames.length);
    Assert.assertEquals("key2", keyNames[0]);
    Assert.assertEquals("key3", keyNames[1]);
  }

  @Test

  public void testGetKeyNamesFromIndexesWithEmptyArray() {
    CoordinationApplicationContext.getProperty();
    String[] keyNames = CoordinationApplicationContext.getKeyNamesFromIndexes(new int[] {});
    Assert.assertNotNull(keyNames);
    Assert.assertEquals(0, keyNames.length);
  }

  @Test

  public void testGetEnvironmentSpecificProperty() {
    Assert.assertEquals(1, CoordinationApplicationContext.getShardingDimensions().length);
  }

  @Test

  public void testGetShardingIndexes() {
    int[] shardingKeyIndexes = CoordinationApplicationContext.getShardingIndexes();
    Assert.assertNotNull(shardingKeyIndexes);
    Assert.assertEquals(1, shardingKeyIndexes.length);
    Assert.assertEquals(0, shardingKeyIndexes[0]);

  }

  @Test

  public void testGetShardingIndexSequence() {
    Assert.assertEquals(-1, CoordinationApplicationContext.getShardingIndexSequence(3));

  }

 
  @Test
  public void testPropertyByOverriding() throws IOException {
    Property<ZkProperty> property =
        new ZkProperty()
            .overrideProperty("./src/main/resources/zookeeper.properties");
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
