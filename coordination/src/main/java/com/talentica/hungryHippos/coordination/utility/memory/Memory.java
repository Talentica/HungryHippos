/**
 * 
 */
package com.talentica.hungryHippos.coordination.utility.memory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;

import com.talentica.hungryHippos.client.domain.DataDescription;


/**
 * {@code Memory} used for allocating memory to Job.
 * @author PooshanS
 *
 */
public interface Memory {

  /**
   * allocates memory for each job.
   * @param dataDescription
   * @return Map<Integer, Long>
   * @throws ClassNotFoundException
   * @throws FileNotFoundException
   * @throws KeeperException
   * @throws InterruptedException
   * @throws IOException
   * @throws JAXBException
   */
  Map<Integer, Long> getJobMemoryAlloc(DataDescription dataDescription) throws ClassNotFoundException, FileNotFoundException,
      KeeperException, InterruptedException, IOException, JAXBException;

}
