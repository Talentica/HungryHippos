package com.talentica.hungryHippos.sharding;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.utility.marshaling.FileReader;
import com.talentica.hungryHippos.utility.marshaling.Reader;

/**
 * @author nitink
 */
public class ShardingTest {

	private Sharding sharding;

	private Reader shardingInputFileReader;

	@Before
	public void setup() throws IOException {
		sharding = new Sharding(5);
		shardingInputFileReader = new FileReader(
				new File("src/test/java/com/talentica/hungryHippos/sharding/testSampleInput.txt"));
		shardingInputFileReader.setNumFields(9);
		shardingInputFileReader.setMaxsize(25);
	}

	@Test
	public void testPopulateFrequencyFromData() throws IOException {
		Map<String, List<KeyValueFrequency>> frequencyData = sharding
				.populateFrequencyFromData(shardingInputFileReader);
		Assert.assertNotNull(frequencyData);
		Assert.assertEquals(3, frequencyData.size());
		List<KeyValueFrequency> key1ValueFrequencyList = frequencyData.get("key1");
		Assert.assertNotNull(key1ValueFrequencyList);
		Assert.assertEquals(5, key1ValueFrequencyList.size());
		List<KeyValueFrequency> key2ValueFrequencyList = frequencyData.get("key2");
		Assert.assertNotNull(key2ValueFrequencyList);
		Assert.assertEquals(5, key2ValueFrequencyList.size());
		List<KeyValueFrequency> key3ValueFrequencyList = frequencyData.get("key3");
		Assert.assertNotNull(key3ValueFrequencyList);
		Assert.assertEquals(6, key1ValueFrequencyList.get(0).getFrequency());
		Assert.assertEquals(6, key2ValueFrequencyList.get(0).getFrequency());
		Assert.assertEquals(12, key3ValueFrequencyList.get(0).getFrequency());
	}

}
