package com.talentica.hungryHippos.sharding;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.utility.marshaling.FileReader;

/**
 * @author nitink
 */
public class ShardingTest {

	private Sharding sharding;

	private FileReader inputFileReader;

	@Before
	public void setup() throws IOException {
		sharding = new Sharding(5);
		inputFileReader = new FileReader("testSampleInput.txt");
	}

	@Test
	public void testPopulateFrequencyFromData() throws IOException {
		// Map<String, List<KeyValueFrequency>> frequencyData =
		// sharding.populateFrequencyFromData(inputFileReader);
		// Assert.assertNotNull(frequencyData);
		// Assert.assertEquals(5, frequencyData.size());
	}

	@After
	public void tearDown() {

	}

}
