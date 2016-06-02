package com.talentica.hungryHippos.sharding;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.InvalidRowExeption;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.coordination.utility.CsvDataParser;
import com.talentica.hungryHippos.coordination.utility.Property;
import com.talentica.hungryHippos.coordination.utility.marshaling.FileReader;
import com.talentica.hungryHippos.coordination.utility.marshaling.Reader;

/**
 * @author nitink
 */
public class ShardingTest {

	private Sharding sharding;

	private Reader shardingInputFileReader;

	@Before
	public void setup() throws IOException {
		sharding = new Sharding(5);
		DataDescription dataDescription = FieldTypeArrayDataDescription.createDataDescription(
				"STRING-1,STRING-1,STRING-1,STRING-1,DOUBLE-0,DOUBLE-0,DOUBLE-0,DOUBLE-0,STRING-3".split(","), 100);
		CsvDataParser csvDataPreprocessor = new CsvDataParser(dataDescription);
		shardingInputFileReader = new FileReader(
				new File("src/test/java/com/talentica/hungryHippos/sharding/testSampleInput.txt"), csvDataPreprocessor);
	}

	@Test
	public void testPopulateFrequencyFromData() throws IOException, InvalidRowExeption {
		Map<String, Map<MutableCharArrayString, Long>> frequencyData = sharding
				.populateFrequencyFromData(shardingInputFileReader);
		Assert.assertNotNull(frequencyData);
		int noOfKeys = Property.getShardingDimensions().length;
		Assert.assertEquals(noOfKeys, frequencyData.size());
		Map<MutableCharArrayString, Long> keyValueFrequencyList = frequencyData.get("key3");
		Assert.assertNotNull(keyValueFrequencyList);
		Assert.assertNotEquals(0, keyValueFrequencyList.size());
		MutableCharArrayString value = new MutableCharArrayString(1);
		value.addCharacter('e');
		Long frequency = keyValueFrequencyList.get(value);
		Assert.assertNotNull(frequency);
		Assert.assertEquals(Long.valueOf(6), frequency);
	}

}
