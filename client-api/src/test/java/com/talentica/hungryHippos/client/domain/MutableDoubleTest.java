package com.talentica.hungryHippos.client.domain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class MutableDoubleTest {

  private static final double DELTA = 1e-15;

  @Test
  public void testMutableDouble() {
    MutableDouble localInteger = new MutableDouble();
    localInteger.addValue(112.34523543435);
    assertNotNull(localInteger);
    assertEquals(8, localInteger.getLength());
    assertEquals(112.34523543435, localInteger.toDouble(), DELTA);
    localInteger.addValue(11232.3245623543435);
    assertEquals(8, localInteger.getLength());
    assertEquals(11232.3245623543435, localInteger.toDouble(), DELTA);
  }
}
