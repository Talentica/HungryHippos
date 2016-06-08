package com.talentica.hungryHippos.sharding;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.coordination.utility.ENVIRONMENT;
import com.talentica.hungryHippos.coordination.utility.Property;
import com.talentica.hungryHippos.coordination.utility.Property.PROPERTIES_NAMESPACE;
import com.talentica.hungryHippos.coordination.utility.marshaling.FileReader;
import com.talentica.hungryHippos.coordination.utility.marshaling.Reader;

/**
 *
 * @author nitink
 */
public class ShardingTest {

	private Reader shardingInputFileReader;

	@Before
	public void setup() throws IOException {

		shardingInputFileReader = new FileReader(
				new File("src/test/java/com/talentica/hungryHippos/sharding/testSampleInput.txt"));
	}

	// The populate frequency can't be test alone as the method is to much
	// coupled
	/*
	 * @Test public void testPopulateFrequencyFromData() throws IOException {
	 * Property.initialize(PROPERTIES_NAMESPACE.MASTER);
	 * ENVIRONMENT.setCurrentEnvironment("LOCAL");
	 * 
	 * Map<String, Map<MutableCharArrayString, Long>> frequencyData =
	 * sharding.populateFrequencyFromData(shardingInputFileReader);
	 * Assert.assertNotNull(frequencyData); int noOfKeys =
	 * Property.getShardingDimensions().length; Assert.assertEquals(noOfKeys,
	 * frequencyData.size()); for (String key : frequencyData.keySet()) {
	 * Map<MutableCharArrayString, Long> keyValueFrequencyList =
	 * frequencyData.get(key); Assert.assertNotNull(keyValueFrequencyList);
	 * Assert.assertNotEquals(0, keyValueFrequencyList.size());
	 * MutableCharArrayString value = new MutableCharArrayString(1);
	 * value.addCharacter('l'); int indexOfL = -1; KeyValueFrequency
	 * keyValueFrequenceKey1L = new KeyValueFrequency(value, 6);
	 * 
	 * } }
	 */

	/**
	 * This method test all the methods written in Sharding. An assert statement
	 * has to be added.
	 * 
	 */
	@Test
	public void testDoSharding() {
		Property.initialize(PROPERTIES_NAMESPACE.MASTER);
		ENVIRONMENT.setCurrentEnvironment("LOCAL");
		Sharding.doSharding(shardingInputFileReader);
	}

	@After
	public void tearDown() {
		try {
			shardingInputFileReader.close();
		} catch (IOException e) {

		}
	}

}
