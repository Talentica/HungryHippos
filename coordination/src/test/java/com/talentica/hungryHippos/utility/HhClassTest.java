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
/**
 * 
 */
package com.talentica.hungryHippos.utility;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author pooshans
 *
 */
public class HhClassTest {

  private char[] chars;
  private List<String> list;
  private Map<String, List<String>> keyValue;

  public HhClassTest() {}

  public Map<String, List<String>> getKeyValue() {
    return keyValue;
  }

  public void setKeyValue(Map<String, List<String>> keyValue) {
    this.keyValue = keyValue;
  }

  public char[] getChars() {
    return chars;
  }

  public void setChars(char[] chars) {
    this.chars = chars;
  }

  public List<String> getList() {
    return list;
  }

  public void setList(List<String> list) {
    this.list = list;
  }

  @Override
  public boolean equals(Object o) {
    if(null == o){
      return false;
    }
    HhClassTest that = (HhClassTest) o;
    if (Arrays.equals(chars, that.getChars())) {
      if (list.containsAll(that.getList())) {
        for (Entry<String, List<String>> entry : that.getKeyValue().entrySet()) {
          String key = entry.getKey();
          if (key == null)
            return false;
          List<String> list = keyValue.get(key);
          if (!list.containsAll(entry.getValue())) {
            return false;
          }
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    for (char c : chars) {
      hash = hash * 31 + c;
    }
    for (String string : list) {
      hash = hash * 31 + string.hashCode();
    }
    hash = hash * 31 + keyValue.hashCode();
    return hash;
  }


}
