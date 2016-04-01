package com.talentica.hungryHippos.sharding;

import java.io.File;
import java.io.IOException;

import org.junit.Before;

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
	}

	// @Test
	// public void testPopulateFrequencyFromData() throws IOException {
	// Map<String, List<KeyValueFrequency>> frequencyData = sharding
	// .populateFrequencyFromData(shardingInputFileReader);
	// Assert.assertNotNull(frequencyData);
	// int noOfKeys = Property.getShardingDimensions().length;
	// Assert.assertEquals(noOfKeys, frequencyData.size());
	// for (String key : frequencyData.keySet()) {
	// List<KeyValueFrequency> keyValueFrequencyList = frequencyData.get(key);
	// Assert.assertNotNull(keyValueFrequencyList);
	// Assert.assertNotEquals(0, keyValueFrequencyList.size());
	// MutableCharArrayString value = new MutableCharArrayString(1);
	// value.addCharacter('l');
	// int indexOfL = -1;
	// KeyValueFrequency keyValueFrequenceKey1L = new KeyValueFrequency(value,
	// 6);
	// if ((indexOfL = keyValueFrequencyList.indexOf(keyValueFrequenceKey1L)) >
	// 0) {
	// Assert.assertEquals(keyValueFrequenceKey1L,
	// keyValueFrequencyList.get(indexOfL));
	// }
	// }
	// }

}
