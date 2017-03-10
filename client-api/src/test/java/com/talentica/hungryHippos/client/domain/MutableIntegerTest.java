package com.talentica.hungryHippos.client.domain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class MutableIntegerTest {
  
  static MutableInteger localInteger = new MutableInteger();
  

  @Test
  public void testMutableInteger() {
    String token =  "-112";
    localInteger.addValue(token);
    assertNotNull(localInteger);
    assertEquals(4, localInteger.getLength());
    assertEquals(-112, localInteger.toInt());
    token = "1134342";
    localInteger.addValue(token);
    assertEquals(4, localInteger.getLength());
    assertEquals(1134342, localInteger.toInt());
  }
  
  @Test
  public void testMutableItegerClone(){
    String token = "112";
    localInteger.addValue(token);
    
    MutableInteger mutableInteger = localInteger.clone();
    token  = "113";
    mutableInteger.addValue(token);
    
    assertEquals(112, localInteger.toInt());
    assertEquals(113, mutableInteger.toInt());
  }

}
