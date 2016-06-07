package com.talentica.hungryHippos.client.data.parser;

import java.io.InputStream;
import java.util.Iterator;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.InvalidRowExeption;
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
public interface DataParser {

	/**
	 * This method gets called by framework to get data blocks (e.g. a row in
	 * CSV file) for publishing to data nodes in the cluster. Please be careful
	 * while using {@link MutableCharArrayString} as it is mutable.
	 * 
	 * @param dataStream
	 * @return
	 * @throws InvalidRowExeption
	 */
	public Iterator<MutableCharArrayString[]> iterator(InputStream dataStream, DataDescription dataDescription)
			throws RuntimeException;

}