package com.talentica.hungryHippos.client.domain;

/**
 * The interface for the description of the data
 * 
 * @author debasishc.
 * @version 0.5.0
 * @since 2015-09-01
 */
public interface DataDescription {

	/**
	 * To get the DataLocator object at particular index.
	 * 
	 * @param index
	 *            is the column of the data set.
	 * @return DataLocator object
	 */
	public DataLocator locateField(int index);

	/**
	 * To get the offset of the value.
	 * 
	 * @return offset for next value.
	 */
	public int getSize();

	/**
	 * To get the array of the key order.
	 * 
	 * @return Array of the key order.
	 */
	public String[] keyOrder();

	/**
	 * To get the number of the data fields of the records.
	 * 
	 * @return the number of the data fields.
	 */
	public int getNumberOfDataFields();

	/**
	 * Returns maximum size of data in number of bytes which can be present in
	 * single processing block e.g. single line in csv file.
	 * 
	 * @return returns an int value representing the single block size.
	 */
	public int getMaximumSizeOfSingleBlockOfData();

}
