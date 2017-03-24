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
package com.talentica.hungryHippos.coordination.domain;

import java.io.IOException;

import org.apache.commons.lang3.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@code LeafBean} is used for storing the zookeper node details.
 * 
 * @author PooshanS
 *
 */
public class LeafBean implements Comparable<LeafBean> {
  private final static Logger LOGGER = LoggerFactory.getLogger(LeafBean.class);
  private String path;
  private String name;
  private Object value;

  /**
   * creates an instance of LeafBean.
   * 
   * @param path
   * @param name
   * @param value
   * @throws ClassNotFoundException
   * @throws IOException
   */
  public LeafBean(String path, String name, byte[] value)
      throws ClassNotFoundException, IOException {
    super();
    this.path = path;
    this.name = name;
    try {
      this.value = (value != null) ? String.valueOf(value) : null;
    } catch (SerializationException ex) {
      LOGGER.error("Unable to deserialize object with path:- {} And ex", path, ex);
    }
  }

  /**
   * retrieves the path.
   * 
   * @return a String.
   */
  public String getPath() {
    return path;
  }

  /**
   * sets a path to the instance.
   * 
   * @param path
   */
  public void setPath(String path) {
    this.path = path;
  }

  /**
   * retrieves the name.
   * 
   * @return String.
   */
  public String getName() {
    return name;
  }

  /**
   * sets the name.
   * 
   * @param name
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * retrieves the value.
   * 
   * @return an Object.
   */
  public Object getValue() {
    return value;
  }

  /**
   * sets the value.
   * 
   * @param value
   */
  public void setValue(Object value) {
    this.value = value;
  }

  /**
   * converts the {@value value} into String.
   * 
   * @return a String.
   */
  public String getStrValue() {
    if (value != null) {
      return String.valueOf(value);
    }
    return null;
  }

  @Override
  public String toString() {
    return "LeafBean [path=" + path + ", name=" + name + ", value=" + value + "]";
  }

  @Override
  public int compareTo(LeafBean o) {
    return (this.path + this.name).compareTo((o.path + o.path));
  }
}
