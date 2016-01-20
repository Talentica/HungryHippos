package com.talentica.hungryHippos.resource.manager;

import java.util.List;
import java.util.Map;

/**
 * The resource manager manages available resources optimally by either
 * maximizing or minimizing resource usage and fulfills resources needs.
 * 
 * @author nitink
 *
 */
public interface ResourceManager {

	/**
	 * Returns map of iteration no. and collection of resource consumers to
	 * allocate resources to in corresponding iteration so that resource
	 * consumers do not end up unavailable resources.
	 * 
	 * @param availableResource
	 * @param resourceConsumers
	 * @return
	 */
	public <V extends Resource> Map<Integer, List<ResourceConsumer>> getResourceConsumersToAllocateResourcesTo(
			Resource availableResource, List<ResourceConsumer> resourceConsumers);

}
