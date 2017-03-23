/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
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

import org.junit.Assert;
import org.junit.Test;

public class MutableCharArrayStringCacheTest {

  private static final MutableCharArrayStringCache MUTABLE_CHAR_ARRAY_STRING_CACHE =
      MutableCharArrayStringCache.newInstance();

  @Test
  public void testGetMutableStringFromCacheOfSameSize() {
    MutableCharArrayString arrayString1 =
        MUTABLE_CHAR_ARRAY_STRING_CACHE.getMutableStringFromCacheOfSize(1);
    Assert.assertNotNull(arrayString1);
    MutableCharArrayString arrayString2 =
        MUTABLE_CHAR_ARRAY_STRING_CACHE.getMutableStringFromCacheOfSize(1);
    Assert.assertNotNull(arrayString2);
    Assert.assertTrue(arrayString1 == arrayString2);
  }

  @Test
  public void testGetMutableStringFromCacheOfDifferentSize() {
    MutableCharArrayString arrayString1 =
        MUTABLE_CHAR_ARRAY_STRING_CACHE.getMutableStringFromCacheOfSize(1);
    Assert.assertNotNull(arrayString1);
    MutableCharArrayString arrayString2 =
        MUTABLE_CHAR_ARRAY_STRING_CACHE.getMutableStringFromCacheOfSize(2);
    Assert.assertNotNull(arrayString2);
    Assert.assertFalse(arrayString1 == arrayString2);
  }

  @Test
  public void testGetMutableStringFromCacheAndChangeCharactersInIt() throws InvalidRowException {
    MutableCharArrayString arrayString1 =
        MUTABLE_CHAR_ARRAY_STRING_CACHE.getMutableStringFromCacheOfSize(4);
    Assert.assertNotNull(arrayString1);
    arrayString1.addByte((byte) 'a');
    arrayString1.addByte((byte) 'b');
    arrayString1.addByte((byte) 'c');
    arrayString1.addByte((byte) 'd');
    Assert.assertEquals(4, arrayString1.length());
    MutableCharArrayString arrayString2 =
        MUTABLE_CHAR_ARRAY_STRING_CACHE.getMutableStringFromCacheOfSize(4);
    arrayString2.addByte((byte) 'e');
    Assert.assertEquals(1, arrayString2.length());
  }

}
