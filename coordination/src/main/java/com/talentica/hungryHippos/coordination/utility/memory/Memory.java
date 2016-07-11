/**
 * 
 */
package com.talentica.hungryHippos.coordination.utility.memory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;


/**
 * @author PooshanS
 *
 */
public interface Memory {

  Map<Integer, Long> getJobMemoryAlloc() throws ClassNotFoundException, FileNotFoundException,
      KeeperException, InterruptedException, IOException, JAXBException;

}
