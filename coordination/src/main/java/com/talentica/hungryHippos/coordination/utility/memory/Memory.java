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
 * @author PooshanS
 *
 */
public interface Memory {

  Map<Integer, Long> getJobMemoryAlloc(DataDescription dataDescription) throws ClassNotFoundException, FileNotFoundException,
      KeeperException, InterruptedException, IOException, JAXBException;

}
