package com.talentica.hungryHippos.sharding;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.client.data.parser.CsvDataParser;
import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.InvalidRowException;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.utility.marshaling.FileReader;
import com.talentica.hungryHippos.coordination.utility.marshaling.Reader;
import com.talentica.hungryhippos.config.coordination.ClusterConfig;
import com.talentica.hungryhippos.config.coordination.Node;

/**
 *
 * @author nitink
 */
public class ShardingTest {

	private Reader shardingInputFileReader;
	private Sharding sharding;

	@Before
	public void setup() throws IOException {
		ClusterConfig clusterConfig = getClusterConfiguration();
		sharding = new Sharding(clusterConfig);
		DataDescription dataDescription = FieldTypeArrayDataDescription.createDataDescription(
				"STRING-1,STRING-1,STRING-1,STRING-1,DOUBLE-0,DOUBLE-0,DOUBLE-0,DOUBLE-0,STRING-3".split(","), 100);
		CsvDataParser csvDataPreprocessor = new CsvDataParser(dataDescription);

		shardingInputFileReader = new FileReader(
				new File("src/test/java/com/talentica/hungryHippos/sharding/testSampleInput.txt"), csvDataPreprocessor);
	}

	private ClusterConfig getClusterConfiguration() {
		com.talentica.hungryhippos.config.coordination.ObjectFactory factory = new com.talentica.hungryhippos.config.coordination.ObjectFactory();
		ClusterConfig clusterConfig = factory.createClusterConfig();
		Node node0 = new Node();
		node0.setIdentifier(0);
		clusterConfig.getNode().add(node0);

		Node node1 = new Node();
		node1.setIdentifier(1);
		clusterConfig.getNode().add(node1);

		Node node2 = new Node();
		node2.setIdentifier(2);
		clusterConfig.getNode().add(node2);
		return clusterConfig;
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
		new Sharding(getClusterConfiguration()).doSharding(shardingInputFileReader);
	}

	@After
	public void tearDown() {
		try {
			shardingInputFileReader.close();
		} catch (IOException e) {

		}
	}

	@Test
	public void testPopulateFrequencyFromData() throws IOException, InvalidRowException {
		Map<String, Map<MutableCharArrayString, Long>> frequencyData = sharding
				.populateFrequencyFromData(shardingInputFileReader);
		Assert.assertNotNull(frequencyData);
		int noOfKeys = CoordinationApplicationContext.getShardingDimensions().length;
		Assert.assertEquals(noOfKeys, frequencyData.size());
		Map<MutableCharArrayString, Long> keyValueFrequencyList = frequencyData.get("key1");
		Assert.assertNotNull(keyValueFrequencyList);
		Assert.assertNotEquals(0, keyValueFrequencyList.size());
		MutableCharArrayString value = new MutableCharArrayString(1);
		value.addCharacter('e');
		Long frequency = keyValueFrequencyList.get(value);
		Assert.assertNotNull(frequency);
		Assert.assertEquals(Long.valueOf(6), frequency);

	}

}
