/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
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
