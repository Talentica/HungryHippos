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
	
	//static int[] numbers = { 1, 4, 45,45, 6, 10, 8, 13, 23, 5, 7, 8, 12 };
	static int[] numbers = { 196105, 81192, 62325, 52239, 46431, 41734, 38683,
			35730, 33624, 31867, 30450, 28672, 27859, 26784, 25481, 24737,
			24172, 23665, 22979, 22140, 21608, 21056, 20894, 20480, 19786,
			19307, 196105, 81192, 62325, 52239, 46431, 41734, 38683, 35730,
			33624, 31867, 30450, 28672, 27859, 26784, 25481, 24737, 24172,
			23665, 22979, 22140, 21608, 21056, 20894, 20480, 19786, 19307 };
	static List<Integer> input = new ArrayList<>(); 
	
	@Before
	public void setUp(){
		this.availableDiskSize = 0l; //unused till now.
		this.availableRam = 196105l; //suppose 196105MB.
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
