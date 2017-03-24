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

public class MutableDoubleTest {

  private static final double DELTA = 1e-15;

  @Test
  public void testMutableDouble() {
    MutableDouble mutableDouble = new MutableDouble();
    System.out.println(mutableDouble.parseDouble("112.34523543435"));
    mutableDouble.addValue("112.34523543435");
    assertNotNull(mutableDouble);
    assertEquals(8, mutableDouble.getLength());
    assertEquals(112.34523543435, mutableDouble.toDouble(), DELTA);
    mutableDouble.addValue("-11232.3245623543435");
    assertEquals(8, mutableDouble.getLength());
    assertEquals(-11232.3245623543435, mutableDouble.toDouble(), DELTA);
    
  }
}
