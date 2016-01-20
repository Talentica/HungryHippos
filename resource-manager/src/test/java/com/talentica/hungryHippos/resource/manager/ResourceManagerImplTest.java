package com.talentica.hungryHippos.resource.manager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class ResourceManagerImplTest {

	@Test
	public void testResourceManagerImpleForSingleIteration() {
		Memory availableMemory = new Memory(10);
		List<ResourceConsumer> consumers = new ArrayList<>();
		TestConsumer consumer = new TestConsumer(new Memory(10));
		consumers.add(consumer);
		ResourceManagerImpl managerImpl = new ResourceManagerImpl();
		Map<Integer, List<ResourceConsumer>> result = managerImpl
				.getResourceConsumersToAllocateResourcesTo(availableMemory, consumers);
		Assert.assertNotNull(result);
		Assert.assertEquals(1, result.size());
		Assert.assertEquals(1, result.get(0).size());
		Assert.assertEquals(consumer, result.get(0).get(0));
	}

	@Test
	public void testResourceManagerImplForTwoIterations() {
		Memory availableMemory = new Memory(10);
		List<ResourceConsumer> consumers = new ArrayList<>();
		TestConsumer consumer1 = new TestConsumer(new Memory(10));
		consumers.add(consumer1);
		TestConsumer consumer2 = new TestConsumer(new Memory(5));
		consumers.add(consumer2);
		TestConsumer consumer3 = new TestConsumer(new Memory(5));
		consumers.add(consumer3);
		ResourceManagerImpl managerImpl = new ResourceManagerImpl();
		Map<Integer, List<ResourceConsumer>> result = managerImpl
				.getResourceConsumersToAllocateResourcesTo(availableMemory, consumers);
		Assert.assertNotNull(result);
		Assert.assertEquals(2, result.size());
		Assert.assertEquals(1, result.get(0).size());
		Assert.assertEquals(consumer1, result.get(0).get(0));
	}

	private static class TestConsumer implements ResourceConsumer {

		private Resource resourceRequirement = null;

		public TestConsumer(Resource resource) {
			this.resourceRequirement = resource;
		}

		@Override
		public Resource getResourceRequirement() {
			return resourceRequirement;
		}

		@Override
		public int compareTo(ResourceConsumer otherResourceConsumer) {
			return getResourceRequirement().compareTo(otherResourceConsumer.getResourceRequirement());
		}

		@Override
		public String toString() {
			return "TestConsumer{Requirement:" + getResourceRequirement() + "}";
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj != null && obj instanceof TestConsumer) {
				TestConsumer consumer = (TestConsumer) obj;
				if (consumer.getResourceRequirement() != null && getResourceRequirement() != null) {
					return consumer.getResourceRequirement().equals(getResourceRequirement());
				}
			}
			return false;
		}

	}
}
