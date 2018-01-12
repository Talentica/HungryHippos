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

/**
 * {@code MutableFloat} is used for memory optimization. Because of this class system will be making
 * less objects for Float.
 * 
 * @author sudarshans
 *
 */
public class MutableFloat implements DataTypes {

  private static final long serialVersionUID = -6085804645390531875L;
  private float floatValue;


  /**
   * creates a new MutableFloat with specified length. The length specified is the limit of the
   * underlying array.
   *
   */
  public MutableFloat() {
  }

  @Override
  public int getLength() {
    return Float.BYTES;
  }


  @Override
  public String toString() {
    return floatValue+"";
  }


  @Override
  public void reset() {

  }

  @Override
  public MutableFloat clone() {
    MutableFloat mutableFloat = new MutableFloat();
    mutableFloat.floatValue = this.floatValue;
    return mutableFloat;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || !(o instanceof MutableFloat)) {
      return false;
    }
    MutableFloat that = (MutableFloat) o;
    return this.floatValue==that.floatValue;
  }

  @Override
  public int hashCode() {
    return Float.hashCode(this.floatValue);
  }

  @Override
  public int compareTo(DataTypes dataType) {
    MutableFloat otherMutableFloat = null;
    if (dataType instanceof MutableFloat) {
      otherMutableFloat = (MutableFloat) dataType;
    } else {
      return -1;
    }
    return Float.compare(this.floatValue,otherMutableFloat.floatValue);
  }

  @Override
  public DataTypes addValue(String value) {
    floatValue = Float.valueOf(value);
    return this;
  }

  public float getFloatValue() {
    return floatValue;
  }
}
