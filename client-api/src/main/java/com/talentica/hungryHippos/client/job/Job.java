package com.talentica.hungryHippos.client.job;

import com.talentica.hungryHippos.client.domain.Work;

/**
 * This interface provides set of criteria required to perform the job matrix
 * operation for any aggregate functions.
 * 
 * @author debasishc
 * @version 0.5.0
 * @since 2015-09-09
 */
public interface Job {

	/**
	 * Method is required to create Work interface to perform actual nature of
	 * the operation of aggregation.
	 * 
	 * @return Work interface
	 */
	Work createNewWork();

	/**
	 * Dimensions actually represents the column of the data set on the basis of
	 * which various underlying operation are performed.
	 * 
	 * @return array of the dimensions of data set.
	 */
	int[] getDimensions();

	/**
	 * Primary dimension of the data set is required to do perform underlying
	 * operation. Primary dimension must be one of the dimensions defined.
	 * 
	 * @return Primary dimension of the data set.
	 */
	int getPrimaryDimension();

}
