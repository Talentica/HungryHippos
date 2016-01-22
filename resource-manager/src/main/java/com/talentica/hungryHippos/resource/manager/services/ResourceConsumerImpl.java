/**
 * 
 */
package com.talentica.hungryHippos.resource.manager.services;

import com.talentica.hungryHippos.resource.manager.domain.ResourceConsumer;
import com.talentica.hungryHippos.resource.manager.domain.ResourceRequirement;

/**
 * @author PooshanS
 *
 */
public class ResourceConsumerImpl implements ResourceConsumer{

	private Long diskSize;

	private Long ram;
	
	private Integer consumerId;

	public ResourceConsumerImpl(long diskSizeNeeded, long ramNeeded, int consumerId){
		this.diskSize = diskSizeNeeded;
		this.ram = ramNeeded;
		this.consumerId = consumerId;
	}
	
	
	@Override
	public ResourceRequirement getResourceRequirement() {
		return new ResourceRequirement(diskSize,ram,consumerId);
	}

}
