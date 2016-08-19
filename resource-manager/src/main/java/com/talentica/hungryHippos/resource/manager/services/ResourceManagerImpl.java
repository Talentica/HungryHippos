package com.talentica.hungryHippos.resource.manager.services;

import java.util.List;
import java.util.Map;

import com.talentica.hungryHippos.resource.manager.domain.ResourceConsumer;

public class ResourceManagerImpl implements ResourceManager {

	@Override
	public Map<Integer, List<ResourceConsumer>> getIterationWiseResourceConsumersToAllocateResourcesTo(Long availableDiskSize,
			Long availableRam, List<ResourceConsumer> resourceConsumers) {
		return new TaskPartitions(resourceConsumers, availableRam).getIterationWiseResourceConsumers();
	}

}
