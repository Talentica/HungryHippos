package com.talentica.hungryHippos.client.data;

import com.talentica.hungryHippos.client.domain.DataDescription;
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
	 * This method gets called by framework for every input data block (e.g. a
	 * row in CSV file) being processed before publishing it to all nodes in the
	 * cluster. Please be careful while using {@link MutableCharArrayString} as
	 * it is mutable.
	 * 
	 * @param data
	 * @return
	 */
	public MutableCharArrayString[] preprocess(MutableCharArrayString data);

	/**
	 * Returns the data description detail of block of data being processed.
	 * 
	 * @return
	 */
	public DataDescription getDataDescription();

}