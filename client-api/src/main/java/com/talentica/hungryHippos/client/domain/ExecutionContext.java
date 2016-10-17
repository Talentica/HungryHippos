package com.talentica.hungryHippos.client.domain;

import java.nio.ByteBuffer;

/**
 * This interface is used to perform various operation on each of the records of
 * the data set provided.
 * 
 * @author debasishc.
 * @since 2015-09-09
 * @version 0.5.0
 */
public interface ExecutionContext {

	/**
	 * Used to get the value in ByteBuffer format.
	 * 
	 * @return value in ByteBuffer
	 */
	ByteBuffer getData();

	/**
	 * Method is used to get the value at particular index of the record of data
	 * set.
	 * 
	 * @param index is the position of the particular value.
	 * @return Value as object at particular index
	 */
	Object getValue(int index);

	/**
	 * To get the String value of the record at particular index of data set.
	 * 
	 * @param index is position of the particular value.
	 * @return Custom mutable char array string
	 */
	MutableCharArrayString getString(int index);

	/**
	 * To save the value of index or column of records after aggregation are
	 * completed
	 * 
	 * @param calculationIndex
	 *            is index on which aggregation is performed.
	 * @param value
	 *            is the value computed value after aggregation.
	 */
	void saveValue(int calculationIndex, Object value);

	/**
	 * @param calculationIndex
	 *            is index on which aggregation is performed.
	 * @param value
	 *            is the value computed value after aggregation.
	 * @param metric
	 *            name of the matrix such as sum, median etc.
	 */
	void saveValue(int calculationIndex, Object value, String metric);
	
	/**
	 * @param jobId
	 * @param calculationIndex
	 * @param value
	 * @param metric
	 */
	void saveValue(int jobId, int calculationIndex , Object value, String metric);
	
	/**
	 * @param value
	 */
	void saveValue(Object value);

	/**
	 * To set the keys for particular key-index value pair.
	 * 
	 * @param valueSet
	 *            Key-index and value pair.
	 */
	void setKeys(ValueSet valueSet);

	/**
	 * To get the ValueSet object.
	 * 
	 * @return ValueSet is Key-index and value pair.
	 */
	ValueSet getKeys();
	
	void flush();
}
