/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
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
package com.talentica.hungryHippos.client.data.parser;

import java.io.InputStream;
import java.util.Iterator;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataTypes;
import com.talentica.hungryHippos.client.domain.InvalidRowException;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;

/**
 * Sometimes data is not in the format required by HungryHippos framework and so
 * needs preprocessing. By implementing or extending few readily available
 * parser implementations you can can parse and convert data into the format
 * required by HungryHippos.
 * 
 * @author nitink
 *
 */
public abstract class DataParser {

	private DataDescription dataDescription;

	public DataParser(DataDescription dataDescription) {
		this.dataDescription = dataDescription;
	}

	/**
	 * This method gets called by framework to get data blocks (e.g. a row in
	 * CSV file) for publishing to data nodes in the cluster. Please be careful
	 * while using {@link MutableCharArrayString} as it is mutable.
	 * 
	 * @param dataStream
	 * @return
	 * @throws InvalidRowException
	 */
	public abstract Iterator<DataTypes[]> iterator(
			InputStream inputStream);

	protected DataDescription getDataDescription() {
		return dataDescription;
	}

}
