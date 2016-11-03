package com.talentica.hungryHippos.resource.manager.services;

import java.util.List;
import java.util.Map;

import com.talentica.hungryHippos.resource.manager.domain.ResourceConsumer;
import com.talentica.hungryHippos.resource.manager.services.ResourceManager;

/**
 * 
 * {@code ResourceManagerImpl} used for managing the resources.
 *
 */
public class ResourceManagerImpl implements ResourceManager {

  /**
   * used for retrieving all the allocated resources.
   */
  @Override
  public Map<Integer, List<ResourceConsumer>> getIterationWiseResourceConsumersToAllocateResourcesTo(
      Long availableDiskSize, Long availableRam, List<ResourceConsumer> resourceConsumers) {
    return new TaskPartitions(resourceConsumers, availableRam).getIterationWiseResourceConsumers();
  }

}
