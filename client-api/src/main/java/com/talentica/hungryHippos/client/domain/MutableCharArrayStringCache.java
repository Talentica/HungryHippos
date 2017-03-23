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

import java.util.HashMap;
import java.util.Map;

/**
 * {@code MutableCharArrayStringCache} is used for storing and retrieving MutableCharArrayString
 * which was created by the system.
 * 
 * @author debashishc
 *
 */
public final class MutableCharArrayStringCache {

  private final Map<Integer, MutableCharArrayString> stringLengthToMutableCharArrayStringCache =
      new HashMap<>();

  private final Map<String, MutableCharArrayString> mutableCharArrayStringCache =
      new HashMap<>(10000);

  private MutableCharArrayStringCache() {}

  public MutableCharArrayString getMutableStringFromCacheOfSize(int size) {
    MutableCharArrayString charArrayString = stringLengthToMutableCharArrayStringCache.get(size);
    if (charArrayString == null) {
      charArrayString = new MutableCharArrayString(size);
      stringLengthToMutableCharArrayStringCache.put(size, charArrayString);

    } else {
      charArrayString.reset();
    }
    return charArrayString;
  }

  public MutableCharArrayString getMutableStringFromCacheOfSize(String alreadyPresent) {
    MutableCharArrayString charArrayString = mutableCharArrayStringCache.get(alreadyPresent);
    return charArrayString;
  }

  public void add(String alreadyPresent, MutableCharArrayString charArrayString) {

    mutableCharArrayStringCache.put(alreadyPresent, charArrayString);

  }

  /**
   * creates a new MutableCharArrayStringCache.
   * 
   * @return a new instance of MutableCharArrayStringCache.
   */
  public static MutableCharArrayStringCache newInstance() {
    return new MutableCharArrayStringCache();
  }

}
