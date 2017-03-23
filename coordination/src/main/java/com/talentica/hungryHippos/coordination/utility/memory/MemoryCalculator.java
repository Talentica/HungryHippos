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
/**
 * 
 */
package com.talentica.hungryHippos.coordination.utility.memory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;

import com.talentica.hungryHippos.client.domain.DataDescription;

/**
 * {@code MemoryCalculator} implements {@code Memory}.
 *
 * @author PooshanS
 *
 */
public class MemoryCalculator implements Memory {

  private Map<Integer, Long> jobIdMemoMap = new HashMap<>();
  private Map<Integer, Long> jobIdRowCountMap;

  public MemoryCalculator(Map<Integer, Long> jobIdRowCountMap) {
    this.jobIdRowCountMap = jobIdRowCountMap;
  }

  @Override
  public Map<Integer, Long> getJobMemoryAlloc(DataDescription dataDescription)
      throws ClassNotFoundException, FileNotFoundException, KeeperException, InterruptedException,
      IOException, JAXBException {
    for (Map.Entry<Integer, Long> e : jobIdRowCountMap.entrySet()) {
      jobIdMemoMap.put(e.getKey(), getObjectSize(e.getValue(), dataDescription));
    }
    return jobIdMemoMap;
  }

  private long getObjectSize(Long rowCount, DataDescription dataDescription)
      throws ClassNotFoundException, FileNotFoundException, KeeperException, InterruptedException,
      IOException, JAXBException {
    return (rowCount * dataDescription.getSize());
  }

}
