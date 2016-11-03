package com.talentica.hungryHippos.resource.manager.domain;

/**
 * {@code ResourceConsumer} used for checking the resource requirment.
 *
 */
public interface ResourceConsumer {

  /**
   * retrieves the ResourceRequirement.
   * 
   * @return
   */
  public ResourceRequirement getResourceRequirement();

}
