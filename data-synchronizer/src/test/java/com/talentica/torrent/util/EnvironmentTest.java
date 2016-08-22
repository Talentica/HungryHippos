package com.talentica.torrent.util;

import org.junit.Assert;
import org.junit.Test;

public class EnvironmentTest {

  @Test
  public void testGetPropertyValue() {
    Assert.assertNotNull(Environment.getPropertyValue("peers.node.path"));
  }

  @Test
  public void testGetCoordinationServerConnectionRetryBaseSleepTimeInMs() {
    Assert.assertTrue(
        Environment.getCoordinationServerConnectionRetryBaseSleepTimeInMs() == 1000);
  }

  @Test
  public void testGetCoordinationServerConnectionRetryMaxTimes() {
    Assert
        .assertTrue(Environment.getCoordinationServerConnectionRetryMaxTimes() == 15);
  }

  @Test
  public void testGetCoordinationServerConnectionRetryMaxTimesWithOverride() {
    System.setProperty("torrent.properties.file.path",
        this.getClass().getClassLoader().getResource("test-application.properties").getPath());
    Environment.reload();
    Assert
        .assertTrue(Environment.getCoordinationServerConnectionRetryMaxTimes() == 20);
  }

}
