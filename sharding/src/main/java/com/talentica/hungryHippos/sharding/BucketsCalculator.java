package com.talentica.hungryHippos.sharding;

import com.talentica.hungryHippos.coordination.utility.Property;

public final class BucketsCalculator {

	private static final String MAXIMUM_SHARD_FILE_SIZE_PROPERTY_KEY = "maximumShardFileSizeInBytes";

	private static final String MAXIMUM_NO_OF_SHARD_BUCKETS_PROPERTY_KEY = "maximumNoOfShardBucketsSize";

	private static final int NO_OF_BYTES_PER_KEY = 4;

	private static final int NO_OF_BYTES_STORING_A_BUCKET_OBJECT_IN_SHARD_TABLE_TAKES = 4;

	public static int calculateNumberOfBucketsNeeded() {
		double MAX_NO_OF_FILE_SIZE = Double
				.valueOf(Property.getPropertyValue(MAXIMUM_SHARD_FILE_SIZE_PROPERTY_KEY).toString());
		int noOfKeys = Property.getShardingDimensions().length;
		long approximateMemoryPerBucketStoredInShardTable = (NO_OF_BYTES_PER_KEY * noOfKeys)
				+ NO_OF_BYTES_STORING_A_BUCKET_OBJECT_IN_SHARD_TABLE_TAKES;
		Double noOfBucketsNeeded = Math.pow(MAX_NO_OF_FILE_SIZE / approximateMemoryPerBucketStoredInShardTable,
				1.0 / noOfKeys);
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
