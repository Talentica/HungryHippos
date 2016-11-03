/**
 * 
 */
package com.talentica.hungryHippos.resource.manager.domain;

import java.util.Comparator;

/**
 * {@code ResourceConsumerComparator} used for comparing ResourceConsumer.
 * 
 * @author PooshanS
 *
 */
public class ResourceConsumerComparator implements Comparator<ResourceConsumer> {

  @Override
  public int compare(ResourceConsumer o1, ResourceConsumer o2) {
    return (int) (o1.getResourceRequirement().getRam() - o2.getResourceRequirement().getRam());
  }

}
