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
