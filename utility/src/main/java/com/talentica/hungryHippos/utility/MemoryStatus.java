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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author PooshanS
 *
 */
public class MemoryStatus {

  private static final Logger LOGGER = LoggerFactory.getLogger(MemoryStatus.class);

  private static final long sparedMemory = 200 * 1024 * 1024;//Memory spared for other activities.

  public synchronized static long getUsableMemory() {
    long availablePrimaryMemory = Runtime.getRuntime().freeMemory();
    return availablePrimaryMemory - sparedMemory;
  }

}
