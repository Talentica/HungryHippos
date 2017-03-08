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
    System.gc();
    long availablePrimaryMemory = Runtime.getRuntime().freeMemory();
    return availablePrimaryMemory - sparedMemory;
  }

}
