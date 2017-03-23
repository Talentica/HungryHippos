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

/**
 * {@code NodeOverflowException } , thrown when ever the Node capacity was reaches more than specified limit.
 *  @author debasishc 
 *  @since 14/8/15.
 */
public class NodeOverflowException extends Exception {

  private static final long serialVersionUID = 408695684941253328L;

  public NodeOverflowException(String message) {
    super(message);
  }
}
