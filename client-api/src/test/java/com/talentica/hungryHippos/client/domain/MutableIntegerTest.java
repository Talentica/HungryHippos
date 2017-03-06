package com.talentica.hungryHippos.client.domain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class MutableIntegerTest {


  @Test
  public void testMutableInteger() {
    MutableInteger localInteger = new MutableInteger();
    localInteger.addValue(112);
    assertNotNull(localInteger);
    assertEquals(4, localInteger.getLength());
    assertEquals(112, localInteger.toInt());
    localInteger.addValue(1134342);
    assertEquals(4, localInteger.getLength());
    assertEquals(1134342, localInteger.toInt());
  }

}
