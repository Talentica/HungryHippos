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

  private static final int _1MB = 1024 * 1024;

  private static final long MAX_MEMORY = Runtime.getRuntime().maxMemory() / _1MB;

  private static final long sparedMemory = 200 * 1024 * 1024;//Memory spared for other activities.

  /**
   * Return memory in MB
   * 
   * @return long
   */
  public static long getFreeMemory() {
    return Runtime.getRuntime().freeMemory() / _1MB;
  }

  public static long getMaximumFreeMemoryThatCanBeAllocated() {
    return (getMaxMemory() - getUsedMemory());
  }

  public static long getUsedMemory() {
    return (getTotalmemory() - getFreeMemory());
  }

  /**
   * Return memory in MB
   * 
   * @return long
   */
  public static long getMaxMemory() {
    return MAX_MEMORY;
  }

  /**
   * Return memory in MB
   * 
   * @return long
   */
  public static long getTotalmemory() {
    return Runtime.getRuntime().totalMemory() / _1MB;
  }

  public static void verifyMinimumMemoryRequirementIsFulfiled(long minimumMemoryToBeAvailable) {
    long freeMemory = MemoryStatus.getMaximumFreeMemoryThatCanBeAllocated();
    if (freeMemory <= minimumMemoryToBeAvailable) {
      LOGGER.error(
          "Either very less memory:{} MBs is available to run jobs or the amount of threshold memory:{} MBs configured is too high.",
          new Object[] {freeMemory, minimumMemoryToBeAvailable});
      throw new RuntimeException(
          "Either very less memory is available to run jobs or the amount of threshold memory configured is too high.");
    }
  }

  public synchronized static long getUsableMemory() {
    System.gc();
    long availablePrimaryMemory = Runtime.getRuntime().freeMemory();
    return availablePrimaryMemory - sparedMemory;
  }

}
