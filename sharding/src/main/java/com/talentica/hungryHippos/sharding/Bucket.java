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
import java.util.Collection;

import com.talentica.hungryHippos.coordination.annotations.ZkTransient;

/**
 * {@code Bucket} used for storing Object count of particular T type.
 *
 * @param <T>
 */
public class Bucket<T> implements Comparable<Bucket<T>>, Serializable {

  @ZkTransient
  private static final long serialVersionUID = 4664630705942019835L;

  private Integer id;

  private long size = 0;

  @ZkTransient
  private long numberOfObjects = 0;

  public Integer getId() {
    return id;
  }

  /**
   * create an empty instance of Bucket.
   */
  public Bucket() {}

  /**
   * create an instance of Bucket with following id and size.
   * 
   * @param id
   * @param size
   */
  public Bucket(int id, long size) {
    this.id = id;
    this.size = size;
  }

  /**
   * create an instance of Bucket with Id.
   * 
   * @param id
   */
  public Bucket(int id) {
    this.id = id;
  }

  /**
   * Add object of T type.
   * 
   * @param t
   */
  public void add(T t) {
    numberOfObjects++;
  }

  /**
   * add all the objects in the collection of T type.
   * 
   * @param t
   */
  public void addAll(Collection<T> t) {
    numberOfObjects = numberOfObjects + t.size();
  }

  /**
   * remove the T type.
   * 
   * @param t
   */
  public void remove(T t) {
    numberOfObjects--;
  }

  /**
   * remove all the Objects of T type belonging to the collection.
   * 
   * @param t
   */
  public void removeAll(Collection<T> t) {
    numberOfObjects = numberOfObjects - t.size();
  }

  /**
   * clear the number of objects to 0.
   */
  public void clear() {
    numberOfObjects = 0;
  }

  /**
   * retrieve the size of this instance.
   * 
   * @return
   */
  public long getSize() {
    return size;
  }

  /**
   * retrieve the number of Objects.
   * 
   * @return
   */
  public Long getFilledSize() {
    return numberOfObjects;
  }

  @Override
  public int hashCode() {
    if (id != null) {
      return id.hashCode();
    }
    return super.hashCode();
  }

  @SuppressWarnings("rawtypes")
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj != null && obj instanceof Bucket) {
      return ((Bucket) obj).getId().equals(getId());
    }
    return false;
  }

  @Override
  public int compareTo(Bucket<T> otherBucket) {
    if (otherBucket != null) {
      return getFilledSize().compareTo(otherBucket.getFilledSize());
    }
    return 0;
  }

  @Override
  public String toString() {
    if (id != null) {
      return "Bucket{" + id + "}";
    }
    return super.toString();
  }

  public long getNumberOfObjects() {
    return numberOfObjects;
  }

  public void setNumberOfObjects(long numberOfObjects) {
    this.numberOfObjects = numberOfObjects;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public void setSize(long size) {
    this.size = size;
  }



}
