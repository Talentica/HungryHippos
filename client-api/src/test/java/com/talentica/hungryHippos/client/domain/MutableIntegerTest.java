/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
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
