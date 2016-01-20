package com.talentica.hungryHippos.resource.manager;

import java.math.BigDecimal;

/**
 * Represents resource in the system which can be measured as some decimal
 * number and which can be utilized to either maximize its usage or minimize its
 * usage. e.g. Memory available to run a job can be thought of as a resource.
 * 
 * @author nitink
 *
 * @param <V>
 */
public interface Resource extends Comparable<Resource> {

	public BigDecimal getValue();

}
