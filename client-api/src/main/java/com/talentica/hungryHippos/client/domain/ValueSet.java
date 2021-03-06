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

import java.io.Serializable;
import java.util.Arrays;

/**
 * This class represent the key-index and value pair.
 * 
 * @version 0.5.0
 * @author debasishc
 * @since 2015-09-09
 */
public class ValueSet implements Comparable<ValueSet>, Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  private int[] keyIndexes;

  private Comparable[] values;

  /**
   * Constructor of ValueSet.
   * 
   * @param keyIndexes is the index of the particular records under aggregation.
   * @param values is the corresponding value of the keyIndexes.
   */
  public ValueSet(int[] keyIndexes, Comparable[] values) {
    this.keyIndexes = keyIndexes;
    setValues(values);
  }

  /**
   * Constructor of ValueSet.
   * 
   * @param keyIndexes is the index of the particular records under aggregation.
   */
  public ValueSet(int[] keyIndexes) {
    this.keyIndexes = keyIndexes;
    this.values = new Comparable[keyIndexes.length];
  }  
 

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    ValueSet valueSet = (ValueSet) o;
    // Probably incorrect - comparing Object[] arrays with Arrays.equals
    //added shortCircuit logic
    return Arrays.equals(values, valueSet.values) && Arrays.equals(keyIndexes, valueSet.keyIndexes);
  }

  public Object[] getValues() {
    return values;
  }

  /**
   * To set the values of the particular keyIndexes.
   * 
   * @param values is the value of the particular keyIndexes.
   */
  public void setValues(Comparable[] values) {
    this.values = values;
    if (values != null) {
      for (int i = 0; i < values.length; i++) {
        setValue(values[i], i);
      }
    }
  }

  /**
   * To set the value at particular index.
   * 
   * @param value at particular index.
   * @param index of the record.
   */
  public void setValue(Object value, int index) {
    if (value instanceof MutableCharArrayString) {
      this.values[index] = ((MutableCharArrayString) value).clone();
    } else {
      this.values[index] = (Comparable) value;
    }
  }

  public int[] getKeyIndexes() {
    return keyIndexes;
  }

  @Override
  public int hashCode() {
    int h = 0;
    int off = 0;
    for (int i = 0; i < keyIndexes.length; i++) {
      h = 31 * h + keyIndexes[off];
      h = h + values[off].hashCode();
      off++;
    }
    return h;
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    if (keyIndexes != null && values != null && keyIndexes.length == values.length) {
      for (int count = 0; count < keyIndexes.length; count++) {
        if (count != 0) {
          result.append("-");
        }
        result.append(values[count]);
      }
    }
    return result.toString();
  }

  @SuppressWarnings("unchecked")
  @Override
  public int compareTo(ValueSet otherValueSet) {
    if (equals(otherValueSet)) {
      return 0;
    }
    if (values != null && otherValueSet.values != null) {
      if (values.length != otherValueSet.values.length) {
        return values.length - otherValueSet.values.length;
      }

      for (int i = 0; i < values.length; i++) {
        if (values[i].equals(otherValueSet.values[i])) {
          continue;
        }
        return values[i].compareTo(otherValueSet.values[i]);
      }
    }
    return 0;
  }
}
