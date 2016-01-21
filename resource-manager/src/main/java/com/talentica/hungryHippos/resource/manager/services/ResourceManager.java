package com.talentica.hungryHippos.resource.manager.services;

import java.util.List;
import java.util.Map;

import com.talentica.hungryHippos.resource.manager.domain.ResourceConsumer;

/**
 * The resource manager manages available resources optimally by either
 * maximizing or minimizing resource usage and fulfills needs of resources.
 * 
 * @author nitink
 *
 */
public interface ResourceManager {

	/**
	 * Returns map of iteration no. and collection of resource consumers to
	 * allocate resources to in corresponding iteration so that resource
	 * consumers do not end up unavailable resources.
	 */
	public Map<Integer, List<ResourceConsumer>> getIterationWiseResourceConsumersToAllocateResourcesTo(Long availableDiskSize,
			Long availableRam, final List<ResourceConsumer> resourceConsumers);

}