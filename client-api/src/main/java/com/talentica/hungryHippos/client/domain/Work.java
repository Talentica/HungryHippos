package com.talentica.hungryHippos.client.domain;

/**
 * This interface provides leverage to perform the nature of different type
 * aggregate operations.
 * 
 * @author debasishc 
 * @version 0.5.0
 * @since 2015-09-09
 */
public interface Work {

	/**
	 * This method need to be implemented to perform particular type of the
	 * aggregation such as sum, median etc.
	 * 
	 * @param executionContext object as parameter.
	 */
	void processRow(ExecutionContext executionContext);

	/**
	 * This method need to be implemented to calculate and save the key-value
	 * pairs in file.
	 * 
	 * @param executionContext object as parameter.
	 */
	void calculate(ExecutionContext executionContext);

	/**
	 * To reset the value of the particular work's aggregation function to zero
	 * (0).
	 */
	void reset();
}
