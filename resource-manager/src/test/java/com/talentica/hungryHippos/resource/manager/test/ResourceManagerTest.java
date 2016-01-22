/**
 * 
 */
package com.talentica.hungryHippos.resource.manager.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.resource.manager.domain.ResourceConsumer;
import com.talentica.hungryHippos.resource.manager.domain.ResourceConsumerComparator;
import com.talentica.hungryHippos.resource.manager.services.ResourceConsumerImpl;
import com.talentica.hungryHippos.resource.manager.services.TaskPartitions;

/**
 * @author PooshanS
 *
 */
public class ResourceManagerTest {
	
	private Long availableDiskSize;
	private Long availableRam;
	private List<ResourceConsumer> resourceConsumers;
	
	static int[] numbers = { 1, 4, 45, 6, 10, 8, 13, 23, 5, 7, 8, 12 };
	static List<Integer> input = new ArrayList<>(); 
	
	@Before
	public void setUp(){
		this.availableDiskSize = 0l; //unused till now.
		this.availableRam = 45l; //suppose 45MB.
		this.resourceConsumers = new ArrayList<ResourceConsumer>();
		feedInput();
	}
	
	@Test
	public void testResourceManager(){
		System.out.println("AVAILABLE RAM = " + availableRam);
		TaskPartitions taskPartitions = new TaskPartitions(resourceConsumers, availableRam);
		for(Entry<Integer, List<ResourceConsumer>> entry : taskPartitions.getIterationWiseResourceConsumers().entrySet()){
			System.out.println("KEY = " + entry.getKey());
			Iterator<ResourceConsumer> itr = entry.getValue().iterator();
			while (itr.hasNext()) {
				ResourceConsumer resourceConsumer = itr.next();
				System.out.println("RESOURCE ID = "
						+ resourceConsumer.getResourceRequirement()
								.getResourceId() + " AND RAM = "
						+ resourceConsumer.getResourceRequirement().getRam());
			}
		    
		}
		
		if(taskPartitions.getOutBoundResources()!=null)System.out.println(taskPartitions.getOutBoundResources());
	}
	
	public void  feedInput(){
		ResourceConsumer resConsumer;
		for(int i = 0 ; i < numbers.length ; i++){
			resConsumer = new ResourceConsumerImpl(0l, numbers[i],i);
			resourceConsumers.add(resConsumer);
		}
		Collections.sort(resourceConsumers,new ResourceConsumerComparator());
	}

}
