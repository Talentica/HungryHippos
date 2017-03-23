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
package com.talentica.hungryHippos.sharding;

import java.io.Serializable;

import com.talentica.hungryHippos.coordination.annotations.ZkTransient;

/**
 * {@code KeyValueFrequency} used for keeping track of numbers of times a particular value has
 * occured.
 * 
 * @author debasishc
 * @since 14/8/15.
 */
public class KeyValueFrequency implements Comparable<KeyValueFrequency>, Serializable {

  @ZkTransient
  private static final long serialVersionUID = 7621688881158875750L;
  private Object keyValue;
  private long frequency;

  /**
   * create an instance of KeyValueFrequency.
   * 
   * @param keyValue
   * @param frequency
   */
  public KeyValueFrequency(Object keyValue, long frequency) {
    this.frequency = frequency;
    this.keyValue = keyValue;
  }

  /**
   * retrieve the frequency.
   * 
   * @return
   */
  public long getFrequency() {
    return frequency;
  }

  /**
   * set the frequency.
   * 
   * @param frequency
   */
  public void setFrequency(long frequency) {
    this.frequency = frequency;
  }

  /**
   * retrieve Object.
   * 
   * @return
   */
  public Object getKeyValue() {
    return keyValue;
  }

  /**
   * set the Object.
   * 
   * @param keyValue
   */
  public void setKeyValue(Object keyValue) {
    this.keyValue = keyValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    KeyValueFrequency that = (KeyValueFrequency) o;

    if (Double.compare(that.frequency, frequency) != 0)
      return false;
    return keyValue.equals(that.keyValue);

  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    result = keyValue.hashCode();
    temp = Double.doubleToLongBits(frequency);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public int compareTo(KeyValueFrequency o) {
    if (frequency > o.frequency) {
      return -1;
    } else if (frequency == o.frequency) {
      return 0;
    } else {
      return 1;
    }
  }

  @Override
  public String toString() {
    return "KeyValueFrequency{" + "frequency=" + frequency + ", keyValue=" + keyValue + '}';
  }

}
