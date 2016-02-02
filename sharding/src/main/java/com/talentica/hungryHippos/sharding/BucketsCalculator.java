package com.talentica.hungryHippos.sharding;

import com.talentica.hungryHippos.utility.Property;

public final class BucketsCalculator {

	private static final String MAXIMUM_SHARD_FILE_SIZE_PROPERTY_KEY = "maximumShardFileSizeInBytes";

	private static final String MAXIMUM_NO_OF_SHARD_BUCKETS_PROPERTY_KEY = "maximumNoOfShardBucketsSize";

	public static int calculateNumberOfBucketsNeeded() {
		double MAX_NO_OF_FILE_SIZE = Double
				.valueOf(Property.getPropertyValue(MAXIMUM_SHARD_FILE_SIZE_PROPERTY_KEY).toString());
		int noOfKeys = Property.getKeyOrder().length;
		long approximateMemoryPerRowBytes = (4 * noOfKeys) + 8;
		Double noOfBucketsNeeded = Math.pow(MAX_NO_OF_FILE_SIZE / approximateMemoryPerRowBytes, 1.0 / noOfKeys);
		int numberOfBucketsNeeded = (int) Math.ceil(noOfBucketsNeeded);
		Object maxNoOfBuckets = Property.getPropertyValue(MAXIMUM_NO_OF_SHARD_BUCKETS_PROPERTY_KEY);
		if (maxNoOfBuckets != null) {
			int maximumNoOfBucketsAllowed = Integer.parseInt(maxNoOfBuckets.toString());
			if (numberOfBucketsNeeded > maximumNoOfBucketsAllowed) {
				numberOfBucketsNeeded = maximumNoOfBucketsAllowed;
			}
		}
		return numberOfBucketsNeeded;
	}

}
