package com.talentica.hungryHippos.client.domain;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class MutableDoubleTest {

  private static final double DELTA = 1e-15;

  @Test
  public void testMutableDouble() {
    MutableDouble mutableDouble = new MutableDouble();
    System.out.println(mutableDouble.parseDouble(new StringBuilder("112.34523543435")));
    mutableDouble.addValue(new StringBuilder("112.34523543435"));
    assertNotNull(mutableDouble);
    assertEquals(8, mutableDouble.getLength());
    assertEquals(112.34523543435, mutableDouble.toDouble(), DELTA);
    mutableDouble.addValue(new StringBuilder("-11232.3245623543435"));
    assertEquals(8, mutableDouble.getLength());
    assertEquals(-11232.3245623543435, mutableDouble.toDouble(), DELTA);
    
  }
}
