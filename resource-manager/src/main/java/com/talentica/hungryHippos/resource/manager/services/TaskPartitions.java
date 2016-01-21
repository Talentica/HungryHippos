/**
 * 
 */
package com.talentica.hungryHippos.resource.manager.services;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.talentica.hungryHippos.resource.manager.domain.ResourceConsumer;

/**
 * Task partition do the greedy selections of the task to make it available to the run based on the resource availability.
 * 
 * @author PooshanS
 *
 */
public final class TaskPartitions {
	
	private List<ResourceConsumer> resourceConsumers;
	private List<ResourceConsumer> outBoundResources;
	private long availableRam;
	
	public TaskPartitions(List<ResourceConsumer> resourceConsumers,long availableRam){
		this.resourceConsumers = resourceConsumers;
		this.availableRam = availableRam;
	}
	
	public Map<Integer, List<ResourceConsumer>> getIterationWiseResourceConsumers(){
		long tempAvailableRam;
		Integer resourceIndex = 0;
		TreeMap<Integer, List<ResourceConsumer>> resourcesPartition = new TreeMap<Integer, List<ResourceConsumer>>();
		for(int index = 0; index < resourceConsumers.size(); index++){
			if(resourceConsumers.get(index).getResourceRequirement().getRam() > availableRam){
				if(outBoundResources == null){
					outBoundResources = new ArrayList<ResourceConsumer>();
				}
				outBoundResources.add(resourceConsumers.get(index));
				resourceConsumers.remove(index);
			}
		}
		while (!resourceConsumers.isEmpty()) {
			List<ResourceConsumer> subsetResorces = new ArrayList<>();
			int length = resourceConsumers.size();
			tempAvailableRam = availableRam;
			for (int index = 0; index < length; index++) {
				ResourceConsumer consumer = resourceConsumers.get(length - 1 - index);
				if(tempAvailableRam == 0){
					continue;
				}
				Long ramPerResource = consumer.getResourceRequirement().getRam();
				if (ramPerResource > tempAvailableRam) {
					continue;
				}
				else if (ramPerResource <= tempAvailableRam) {
					subsetResorces.add(consumer);
					tempAvailableRam = tempAvailableRam - ramPerResource;
					resourceConsumers.remove((length - 1) - index);
				}  
			}
			resourcesPartition.put(resourceIndex++, subsetResorces);
		}
		
		return resourcesPartition;
	}

	public List<ResourceConsumer> getResourceConsumers() {
		return resourceConsumers;
	}

	public void setResourceConsumers(List<ResourceConsumer> resourceConsumers) {
		this.resourceConsumers = resourceConsumers;
	}

	public List<ResourceConsumer> getOutBoundResources() {
		return outBoundResources;
	}

	public void setOutBoundResources(List<ResourceConsumer> outBoundResources) {
		this.outBoundResources = outBoundResources;
	}

	public long getAvailableRam() {
		return availableRam;
	}

	public void setAvailableRam(long availableRam) {
		this.availableRam = availableRam;
	} 
	
	

}
