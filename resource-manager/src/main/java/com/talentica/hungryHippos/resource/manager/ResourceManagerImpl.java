package com.talentica.hungryHippos.resource.manager;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

public class ResourceManagerImpl implements ResourceManager {

	@Override
	public <V extends Resource> Map<Integer, List<ResourceConsumer>> getResourceConsumersToAllocateResourcesTo(
			Resource availableResource, List<ResourceConsumer> resourceConsumers) {
		Map<Integer, List<ResourceConsumer>> consumers = new HashMap<>();

		PriorityQueue<ResourceConsumer> lhsPriorityQueue = new PriorityQueue<>();
		PriorityQueue<ResourceConsumer> rhsPriorityQueue = new PriorityQueue<>();
		PriorityQueue<ResourceConsumer> centerPriorityQueue = new PriorityQueue<>();

		BigDecimal midConsumerSize = availableResource.getValue().divide(BigDecimal.valueOf(2));

		for (ResourceConsumer consumer : resourceConsumers) {
			if (consumer.getResourceRequirement().getValue().compareTo(midConsumerSize) > 1) {
				lhsPriorityQueue.offer(consumer);
			}
			if (consumer.getResourceRequirement().getValue().compareTo(midConsumerSize) < 1) {
				rhsPriorityQueue.offer(consumer);
			} else {
				centerPriorityQueue.offer(consumer);
			}
		}

		ResourceConsumer[] lhsAsArray = new ResourceConsumer[lhsPriorityQueue.size()];
		lhsAsArray = lhsPriorityQueue.toArray(lhsAsArray);
		List<ResourceConsumer> lhs = Arrays.asList(lhsAsArray);

		ResourceConsumer[] rhsAsArray = new ResourceConsumer[rhsPriorityQueue.size()];
		rhsAsArray = rhsPriorityQueue.toArray(rhsAsArray);
		List<ResourceConsumer> rhs = Arrays.asList(rhsAsArray);

		int iteration = 0;

		Iterator<ResourceConsumer> lhsIterator = lhs.iterator();

		while (lhsIterator.hasNext()) {
			ResourceConsumer lhsResourceConsumer = lhsIterator.next();
			List<ResourceConsumer> iterationResourceConsumersList = new ArrayList<>();
			BigDecimal resourceRequirementTotal = lhsResourceConsumer.getResourceRequirement().getValue();
			iterationResourceConsumersList.add(lhsResourceConsumer);
			consumers.put(iteration, iterationResourceConsumersList);
			while (true) {
				if (resourceRequirementTotal.compareTo(availableResource.getValue()) >= 0
						|| lhsPriorityQueue.isEmpty()) {
					break;
				}

				ResourceConsumer matchingElementOnRhs = findElementOnOppositeSideWithMatchingIndex(
						rhsAsArray, lhsResourceConsumer, midConsumerSize);
				if (matchingElementOnRhs != null) {
					iterationResourceConsumersList.add(matchingElementOnRhs);
					break;
				} else {

				}

				ResourceConsumer newConsumer = lhsPriorityQueue.peek();
				BigDecimal currentConsumerValue = newConsumer.getResourceRequirement().getValue();
				BigDecimal newTotalResourceSize = BigDecimal.valueOf(resourceRequirementTotal.doubleValue());
				newTotalResourceSize.add(currentConsumerValue);
				if (newTotalResourceSize.compareTo(availableResource.getValue()) <= 0) {
					iterationResourceConsumersList.add(newConsumer);
				}
			}
			iteration++;
		}
		return consumers;
	}

	private static ResourceConsumer findElementOnOppositeSideWithMatchingIndex(ResourceConsumer[] oppositeSideElements,
			ResourceConsumer elementToGetOppositeIndexFor,
			BigDecimal zerothIndexSize) {
		ResourceConsumer result = null;
		BigDecimal distanceFromZerothIndex= BigDecimal.valueOf(Math.abs(zerothIndexSize.subtract(elementToGetOppositeIndexFor.getResourceRequirement().getValue()).doubleValue()));
		for (ResourceConsumer resourceConsumer : oppositeSideElements) {
			BigDecimal rhsSize = resourceConsumer.getResourceRequirement().getValue();
			BigDecimal distanceFromZerothIndexForCurrentElement = zerothIndexSize.subtract(rhsSize);
			if (distanceFromZerothIndexForCurrentElement.equals(distanceFromZerothIndex)) {
				result = resourceConsumer;
				break;
			}
		}
		return result;
	}

	private static ResourceConsumer findNearestElementOnOppositeSide(ResourceConsumer[] oppositeSideElements,
			ResourceConsumer elementToGetOppositeIndexFor,
			BigDecimal zerothIndexSize) {
		ResourceConsumer result = null;
		BigDecimal distanceFromZerothIndex = BigDecimal.valueOf(Math.abs(zerothIndexSize
				.subtract(elementToGetOppositeIndexFor.getResourceRequirement().getValue()).doubleValue()));
		for (ResourceConsumer resourceConsumer : oppositeSideElements) {
			BigDecimal rhsSize = resourceConsumer.getResourceRequirement().getValue();
			BigDecimal distanceFromZerothIndexForCurrentElement = zerothIndexSize.subtract(rhsSize);
			if (distanceFromZerothIndexForCurrentElement.equals(distanceFromZerothIndex)) {
				result = resourceConsumer;
				break;
			}
		}
		return result;
	}

}
