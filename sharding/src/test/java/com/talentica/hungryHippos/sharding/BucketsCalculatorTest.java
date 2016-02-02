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
		Assert.assertEquals(100, BucketsCalculator.calculateNumberOfBucketsNeeded());
	}

}
