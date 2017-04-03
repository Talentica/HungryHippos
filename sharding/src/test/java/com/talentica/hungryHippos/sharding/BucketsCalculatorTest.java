/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.talentica.hungryHippos.sharding;

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
	public void testGetBucketNumberForValue() {/*
		BucketsCalculator bucketsCalculator = new BucketsCalculator();
		Bucket<KeyValueFrequency> bucketNumberForValue = bucketsCalculator.getBucketNumberForValue("key1",
				"polygenelubricants");
		Assert.assertNotNull(bucketNumberForValue);
		Assert.assertEquals(Integer.valueOf(0), bucketNumberForValue.getId());
	*/}

}
