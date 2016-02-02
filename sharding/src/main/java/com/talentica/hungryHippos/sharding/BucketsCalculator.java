package com.talentica.hungryHippos.sharding;

public final class BucketsCalculator {

	static final String MAXIMUM_SHARD_FILE_SIZE_PROPERTY_Key = "maximumShardFileSizeInBytes";

	public static int calculateNumberOfBucketsNeeded() {
		// double MAX_NO_OF_FILE_SIZE = Double
		// .valueOf(Property.getPropertyValue(MAXIMUM_SHARD_FILE_SIZE_PROPERTY_Key).toString());
		// int noOfKeys = Property.getKeyOrder().length;
		// long approximateMemoryPerRowBytes = (4 * noOfKeys) + 8;
		// Double noOfBucketsNeeded = Math.pow(MAX_NO_OF_FILE_SIZE /
		// approximateMemoryPerRowBytes, 1.0 / noOfKeys);
		// return (int) Math.ceil(noOfBucketsNeeded);
		return 100;
	}

}
