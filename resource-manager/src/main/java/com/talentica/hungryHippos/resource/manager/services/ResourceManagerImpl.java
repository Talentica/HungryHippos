package com.talentica.hungryHippos.resource.manager.services;

import java.util.List;
import java.util.Map;

import com.talentica.hungryHippos.resource.manager.domain.ResourceConsumer;
import com.talentica.hungryHippos.resource.manager.services.ResourceManager;

public class ResourceManagerImpl implements ResourceManager {

	@Override
	public Map<Integer, List<ResourceConsumer>> getIterationWiseResourceConsumersToAllocateResourcesTo(Long availableDiskSize,
			Long availableRam, List<ResourceConsumer> resourceConsumers) {
		return null;
	}

}
