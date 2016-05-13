package com.talentica.hungryHippos.sharding;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.talentica.hungryHippos.coordination.utility.Property;
import com.talentica.hungryHippos.coordination.utility.Property.PROPERTIES_NAMESPACE;

@Ignore
public class BucketsCalculatorTest {

	@Test
	public void testCalculateNumberOfBucketsNeeded() {
		Property.initialize(PROPERTIES_NAMESPACE.MASTER);
		Property.setOrOverrideConfigurationProperty("common.sharding_dimensions",
				"key1,key2,key3");
		Property.setOrOverrideConfigurationProperty("environment", "TEST");
		Property.setOrOverrideConfigurationProperty("master.maximumShardFileSizeInBytes", "20000000");
		int numberOfBucketsNeeded = BucketsCalculator.calculateNumberOfBucketsNeeded();
		Assert.assertEquals(108, numberOfBucketsNeeded);
		Assert.assertTrue(numberOfBucketsNeeded <= 1000);
	}

	@Test
	public void testCalculateNumberOfBucketsNeededIfBucketsCountIsExceeding() {
		Property.initialize(PROPERTIES_NAMESPACE.MASTER);
		Property.setOrOverrideConfigurationProperty("common.sharding_dimensions", "key1");
		Property.setOrOverrideConfigurationProperty("environment", "TEST");
		int numberOfBucketsNeeded = BucketsCalculator.calculateNumberOfBucketsNeeded();
		Assert.assertEquals(1000, numberOfBucketsNeeded);
	}

}
