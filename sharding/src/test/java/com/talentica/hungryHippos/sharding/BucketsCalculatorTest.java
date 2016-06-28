package com.talentica.hungryHippos.sharding;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class BucketsCalculatorTest {

	@Test
	@Ignore
	public void testCalculateNumberOfBucketsNeeded() {/*
		PropertyOld.setOrOverrideConfigurationProperty("sharding_dimensions", "key1,key2,key3");
		PropertyOld.setOrOverrideConfigurationProperty("environment", "TEST");
		PropertyOld.setOrOverrideConfigurationProperty("master.maximumShardFileSizeInBytes", "20000000");
		int numberOfBucketsNeeded = BucketsCalculator.calculateNumberOfBucketsNeeded();
		Assert.assertEquals(50, numberOfBucketsNeeded);
		Assert.assertTrue(numberOfBucketsNeeded <= 1000);
	*/}

	@Test
	@Ignore
	public void testCalculateNumberOfBucketsNeededIfBucketsCountIsExceeding() {/*
		PropertyOld.initialize(PROPERTIES_NAMESPACE.MASTER);
		PropertyOld.setOrOverrideConfigurationProperty("sharding_dimensions", "key1");
		PropertyOld.setOrOverrideConfigurationProperty("environment", "TEST");
		int numberOfBucketsNeeded = BucketsCalculator.calculateNumberOfBucketsNeeded();
		Assert.assertEquals(50, numberOfBucketsNeeded);
	*/}

	@Test
	@Ignore
	public void testGetBucketNumberForValue() {
		BucketsCalculator bucketsCalculator = new BucketsCalculator();
		Bucket<KeyValueFrequency> bucketNumberForValue = bucketsCalculator.getBucketNumberForValue("key1",
				"polygenelubricants");
		Assert.assertNotNull(bucketNumberForValue);
		Assert.assertEquals(Integer.valueOf(0), bucketNumberForValue.getId());
	}

}
