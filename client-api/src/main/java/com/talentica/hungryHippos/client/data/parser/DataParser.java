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
