package com.talentica.hungryHippos.sharding;

import org.junit.Assert;
import org.junit.Test;

import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.Property.PROPERTIES_NAMESPACE;

public class BucketsCalculatorTest {

	@Test
	public void testCalculateNumberOfBucketsNeeded() {
		Property.setNamespace(PROPERTIES_NAMESPACE.MASTER);
		Property.setOrOverrideConfigurationProperty("common.keyorder",
				"key1,key2,key3");
		int numberOfBucketsNeeded = BucketsCalculator.calculateNumberOfBucketsNeeded();
		Assert.assertEquals(100, numberOfBucketsNeeded);
		Assert.assertTrue(numberOfBucketsNeeded <= 1000);
	}

	@Test
	public void testCalculateNumberOfBucketsNeededIfBucketsCountIsExceeding() {
		Property.setNamespace(PROPERTIES_NAMESPACE.MASTER);
		Property.setOrOverrideConfigurationProperty("common.keyorder", "key1");
		int numberOfBucketsNeeded = BucketsCalculator.calculateNumberOfBucketsNeeded();
		Assert.assertEquals(1000, numberOfBucketsNeeded);
	}

}