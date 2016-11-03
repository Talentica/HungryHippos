/**
 * 
 */
package com.talentica.hungryHippos.resource.manager.services;

import com.talentica.hungryHippos.resource.manager.domain.ResourceConsumer;
import com.talentica.hungryHippos.resource.manager.domain.ResourceRequirement;

/**
 * {@code ResourceConsumerImpl} used to keep track of the resource.
 * 
 * @author PooshanS
 *
 */
public class ResourceConsumerImpl implements ResourceConsumer {

  private Long diskSize;

  private Long ram;

  private Integer consumerId;

  /**
   * creates an instance of ResourceConsumerImpl.
   * 
   * @param diskSizeNeeded
   * @param ramNeeded
   * @param consumerId
   */
  public ResourceConsumerImpl(long diskSizeNeeded, long ramNeeded, int consumerId) {
    this.diskSize = diskSizeNeeded;
    this.ram = ramNeeded;
    this.consumerId = consumerId;
  }


  @Override
  public ResourceRequirement getResourceRequirement() {
    return new ResourceRequirement(diskSize, ram, consumerId);
  }


  @Override
  public String toString() {
    return "ResourceConsumerImpl [diskSize=" + diskSize + ", ram=" + ram + ", consumerId="
        + consumerId + "]";
  }


}
