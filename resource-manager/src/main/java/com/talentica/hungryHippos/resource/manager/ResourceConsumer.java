package com.talentica.hungryHippos.resource.manager;

/**
 * Represents anything that needs {@link Resource} to perform its work. e.g. To
 * run a job, you need certain resources - memory, processor etc. Then such job
 * is a resource consumer of memory/processor resources.
 * 
 * @author nitink
 *
 */
public interface ResourceConsumer extends Comparable<ResourceConsumer> {
	
	public Resource getResourceRequirement();

}