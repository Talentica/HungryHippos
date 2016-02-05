package com.talentica.hungryHippos.sharding;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.utility.CommonUtil;
import com.talentica.hungryHippos.utility.MapUtils;
import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.marshaling.Reader;

/**
 * Created by debasishc on 14/8/15.
 */
public class Sharding {

	private static final Logger LOGGER = LoggerFactory.getLogger(Sharding.class);

	// Map<key1,{KeyValueFrequency(value1,10),KeyValueFrequency(value2,11)}>
	private Map<String, List<Bucket<KeyValueFrequency>>> keysToListOfBucketsMap = new HashMap<>();
	private Map<String, Map<MutableCharArrayString, Long>> keyValueFrequencyMap = new HashMap<>();
	// Map<Key1,Map<value1,Node(1)>
	private Map<String, Map<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap = new HashMap<>();
	PriorityQueue<Node> fillupQueue = new PriorityQueue<>(new NodeRemainingCapacityComparator());

	// e.g. Map<KeyCombination({key1,value1},{key2,value2},{key3,value3}),count>
	private Map<BucketCombination, Long> bucketCombinationFrequencyMap = new HashMap<>();
	private Map<Node, List<BucketCombination>> nodeToKeyMap = new HashMap<>();
	private Map<BucketCombination, Set<Node>> bucketCombinationToNodeNumbersMap = new HashMap<>();
	private Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap = new HashMap<>();

	public final static String bucketToNodeNumberMapFile = "bucketToNodeNumberMap";
	public final static String bucketCombinationToNodeNumbersMapFile = "bucketCombinationToNodeNumbersMap";
	public final static String keyToValueToBucketMapFile = "keyToValueToBucketMap";

	public Sharding(int numNodes) {
		for (int i = 0; i < numNodes; i++) {
			Node node = new Node(300000, i);
			fillupQueue.offer(node);
			nodeToKeyMap.put(node, new ArrayList<BucketCombination>());
		}
	}

	public static void doSharding(Reader input) {
		LOGGER.info("SHARDING STARTED");
		Sharding sharding = new Sharding(Property.getTotalNumberOfNodes());
		try {
			sharding.populateFrequencyFromData(input);
			sharding.populateKeysToListOfBucketsMap();
			sharding.updateBucketToNodeNumbersMap(input);
			sharding.shardAllKeys();
			CommonUtil.dumpFileOnDisk(Sharding.bucketCombinationToNodeNumbersMapFile,
					sharding.bucketCombinationToNodeNumbersMap);
			CommonUtil.dumpFileOnDisk(Sharding.bucketToNodeNumberMapFile, sharding.bucketToNodeNumberMap);
			CommonUtil.dumpFileOnDisk(Sharding.keyToValueToBucketMapFile, sharding.keyToValueToBucketMap);
			LOGGER.info("keyToValueToBucketMap:" + MapUtils.getFormattedString(sharding.keyToValueToBucketMap));
			LOGGER.info("bucketCombinationToNodeNumbersMap: "
					+ MapUtils.getFormattedString(sharding.bucketCombinationToNodeNumbersMap));
		} catch (IOException | NodeOverflowException e) {
			e.printStackTrace();
			LOGGER.error("Error occurred during sharding process.", e);
		}
	}

	private void updateBucketToNodeNumbersMap(Reader data) throws IOException {
		data.reset();
		String[] keys = Property.getKeyOrder();
		// Map<key1,Map<value1,count>>
		while (true) {
			MutableCharArrayString[] parts = data.read();
			if (parts == null) {
				data.close();
				break;
			}
			MutableCharArrayString[] values = new MutableCharArrayString[3];
			values[0] = parts[0].clone();
			values[1] = parts[1].clone();
			values[2] = parts[2].clone();
			Map<String, Bucket<KeyValueFrequency>> bucketCombinationMap = new HashMap<>();
			for (int i = 0; i < keys.length; i++) {
				Bucket<KeyValueFrequency> bucket = keyToValueToBucketMap.get(keys[i]).get(values[i]);
				bucketCombinationMap.put(keys[i], bucket);
			}
			BucketCombination keyCombination = new BucketCombination(bucketCombinationMap);
			Long count = bucketCombinationFrequencyMap.get(keyCombination);
			if (count == null) {
				bucketCombinationFrequencyMap.put(keyCombination, 1L);
			} else {
				bucketCombinationFrequencyMap.put(keyCombination, count + 1);
			}
		}
	}

	// TODO: This method needs to be generalized
	Map<String, Map<MutableCharArrayString, Long>> populateFrequencyFromData(Reader data) throws IOException {
		String[] keys = Property.getKeyOrder();
		// Map<key1,Map<value1,count>>
		while (true) {
			MutableCharArrayString[] parts = data.read();
			if (parts == null) {
				break;
			}
			MutableCharArrayString[] values = new MutableCharArrayString[3];
			values[0] = parts[0].clone();
			values[1] = parts[1].clone();
			values[2] = parts[2].clone();
			for (int i = 0; i < keys.length; i++) {
				Map<MutableCharArrayString, Long> frequencyPerValue = keyValueFrequencyMap.get(keys[i]);
				if (frequencyPerValue == null) {
					frequencyPerValue = new HashMap<>();
					keyValueFrequencyMap.put(keys[i], frequencyPerValue);
				}
				Long frequency = frequencyPerValue.get(values[i]);
				if (frequency == null) {
					frequency = 0L;
				}
				frequencyPerValue.put(values[i], frequency + 1);
			}
		}
		return keyValueFrequencyMap;
	}

	private Map<String, List<Bucket<KeyValueFrequency>>> populateKeysToListOfBucketsMap() {
		String[] keys = Property.getKeyOrder();
		int totalNoOfBuckets = BucketsCalculator.calculateNumberOfBucketsNeeded();
		Map<String, List<KeyValueFrequency>> keyToListOfKeyValueFrequency = getSortedKeyToListOfKeyValueFrequenciesMap();
		for (int i = 0; i < keys.length; i++) {
			Map<Object, Bucket<KeyValueFrequency>> valueToBucketMap = new HashMap<>();
			keyToValueToBucketMap.put(keys[i], valueToBucketMap);
			long frequencyOfAlreadyAddedValues = 0;
			int bucketCount = 0;
			Map<MutableCharArrayString, Long> frequencyPerValue = keyValueFrequencyMap.get(keys[i]);
			long idealAverageSizeOfOneBucket = getSizeOfOnBucket(frequencyPerValue, totalNoOfBuckets);
			LOGGER.info("Ideal size of bucket for {}:{}", new Object[] { keys[i], idealAverageSizeOfOneBucket });
			Bucket<KeyValueFrequency> bucket = new Bucket<>(bucketCount, idealAverageSizeOfOneBucket);
			List<KeyValueFrequency> sortedKeyValueFrequencies = keyToListOfKeyValueFrequency.get(keys[i]);
			List<Bucket<KeyValueFrequency>> buckets = new ArrayList<>();
			if (!sortedKeyValueFrequencies.isEmpty()) {
				long sizeOfOneBucket = idealAverageSizeOfOneBucket;
				KeyValueFrequency maxKeyValueFreq = sortedKeyValueFrequencies.get(0);
				long maximumFrequency = maxKeyValueFreq.getFrequency();
				if (idealAverageSizeOfOneBucket < maximumFrequency) {
					sizeOfOneBucket = maximumFrequency;
					LOGGER.info("Frequency of value {} exceeded ideal size of bucket: {}",
							new Object[] { maxKeyValueFreq.getKeyValue(), maximumFrequency });
				}
				LOGGER.info("Actual bucket size for {}:{}", new Object[] { keys[i], sizeOfOneBucket });
				buckets.add(bucket);
				for (KeyValueFrequency keyValueFrequency : sortedKeyValueFrequencies) {
					Long frequency = keyValueFrequency.getFrequency();
					if (frequencyOfAlreadyAddedValues + frequency > sizeOfOneBucket) {
						bucket = new Bucket<>(bucketCount++, sizeOfOneBucket);
						buckets.add(bucket);
						frequencyOfAlreadyAddedValues = 0;
					}
					frequencyOfAlreadyAddedValues = frequencyOfAlreadyAddedValues + frequency;
					bucket.add(keyValueFrequency);
					valueToBucketMap.put(keyValueFrequency.getKeyValue(), bucket);
				}
				this.keysToListOfBucketsMap.put(keys[i], buckets);
			}
		}
		LOGGER.info("keyValueFrequencyMap: " + MapUtils.getFormattedString(keyValueFrequencyMap));
		return this.keysToListOfBucketsMap;
	}

	public Map<String, List<KeyValueFrequency>> getSortedKeyToListOfKeyValueFrequenciesMap() {
		Map<String, List<KeyValueFrequency>> keyToListOfKeyValueFrequency = new HashMap<>();
		String[] keys = Property.getKeyOrder();
		for (String key : keys) {
			List<KeyValueFrequency> frequencies = new ArrayList<>();
			Map<MutableCharArrayString, Long> keyValueToFrequencyMap = keyValueFrequencyMap.get(key);
			for (MutableCharArrayString keyValue : keyValueToFrequencyMap.keySet()) {
				frequencies.add(new KeyValueFrequency(keyValue, keyValueToFrequencyMap.get(keyValue)));
			}
			Collections.sort(frequencies);
			keyToListOfKeyValueFrequency.put(key, frequencies);
		}
		return keyToListOfKeyValueFrequency;
	}

	private long getSizeOfOnBucket(Map<MutableCharArrayString, Long> frequencyPerValue, int noOfBuckets) {
		long sizeOfOneBucket = 0;
		long totalofAllKeyValueFrequencies = 0;
		for (MutableCharArrayString mutableCharArrayString : frequencyPerValue.keySet()) {
			totalofAllKeyValueFrequencies = totalofAllKeyValueFrequencies
					+ frequencyPerValue.get(mutableCharArrayString);
			sizeOfOneBucket = totalofAllKeyValueFrequencies / (noOfBuckets - 1);
		}
		return sizeOfOneBucket;
	}

	private void shardSingleKey(String keyName) throws NodeOverflowException {
		List<Bucket<KeyValueFrequency>> buckets = keysToListOfBucketsMap.get(keyName);
		Map<Bucket<KeyValueFrequency>, Node> bucketToNodeNumber = new HashMap<>();
		bucketToNodeNumberMap.put(keyName, bucketToNodeNumber);
		Collections.sort(buckets);
		for (Bucket<KeyValueFrequency> bucket : buckets) {
			Node mostEmptyNode = fillupQueue.poll();
			List<BucketCombination> currentKeys = nodeToKeyMap.get(mostEmptyNode);
			if (currentKeys == null) {
				currentKeys = new ArrayList<>();
				nodeToKeyMap.put(mostEmptyNode, currentKeys);
			}
			long currentSize = sumForKeyCombinationUnion(currentKeys);
			Map<String, Bucket<KeyValueFrequency>> wouldBeMap = new HashMap<>();
			wouldBeMap.put(keyName, bucket);
			currentKeys.add(new BucketCombination(wouldBeMap));
			long wouldBeSize = sumForKeyCombinationUnion(currentKeys);
			mostEmptyNode.fillUpBy(wouldBeSize - currentSize);
			fillupQueue.offer(mostEmptyNode);
			bucketToNodeNumber.put(bucket, mostEmptyNode);
		}
	}

	private long sumForKeyCombinationUnion(List<BucketCombination> keyCombination) {
		long sum = 0;
		for (Map.Entry<BucketCombination, Long> entry : bucketCombinationFrequencyMap.entrySet()) {
			BucketCombination keyCombination1 = entry.getKey();
			if (keyCombination1.checkMatchOr(keyCombination)) {
				sum += entry.getValue();
			}
		}
		return sum;
	}

	public void shardAllKeys() throws NodeOverflowException {
		for (String key : keysToListOfBucketsMap.keySet()) {
			LOGGER.info("Sharding on key: {}", key);
			shardSingleKey(key);
		}
		LOGGER.info("bucketToNodeNumberMap:" + MapUtils.getFormattedString(bucketToNodeNumberMap));
		List<String> keyNameList = new ArrayList<>();
		keyNameList.addAll(keysToListOfBucketsMap.keySet());
		makeShardingTable(new BucketCombination(new HashMap<String, Bucket<KeyValueFrequency>>()), keyNameList);
	}

	private void makeShardingTable(BucketCombination source, List<String> keyNames) throws NodeOverflowException {
		String keyName;
		if (keyNames.size() >= 1) {
			keyName = keyNames.get(0);
		} else {
			keyName = null;
		}
		if (keyName == null) {
			// exit case
			// lets check which nodes it goes.
			Set<Node> nodesForBucketCombination = new HashSet<>();
			for (Map.Entry<String, Bucket<KeyValueFrequency>> buckets : source.getBucketsCombination().entrySet()) {
				Node n = bucketToNodeNumberMap.get(buckets.getKey()).get(buckets.getValue());
				if (n != null) {
					nodesForBucketCombination.add(n);
				}
			}
			int numberOfIntersectionStorage = source.getBucketsCombination().size() - nodesForBucketCombination.size();
			Set<Node> nodesToPutBack = new HashSet<>();
			while (numberOfIntersectionStorage > 0) {
				Node mostEmptyNode = fillupQueue.poll();
				if (!nodesForBucketCombination.contains(mostEmptyNode)) {
					nodesForBucketCombination.add(mostEmptyNode);
					Long value = bucketCombinationFrequencyMap.get(source);
					if (value == null) {
						value = 0l;
					}
					mostEmptyNode.fillUpBy(value);
					numberOfIntersectionStorage--;
				}
				nodesToPutBack.add(mostEmptyNode);
			}
			nodesToPutBack.forEach(n -> fillupQueue.offer(n));
			bucketCombinationToNodeNumbersMap.put(source, nodesForBucketCombination);
		} else {
			List<String> restOfKeyNames = new LinkedList<>();
			restOfKeyNames.addAll(keyNames);
			restOfKeyNames.remove(keyName);
			List<Bucket<KeyValueFrequency>> buckets = keysToListOfBucketsMap.get(keyName);
			for (Bucket<KeyValueFrequency> bucket : buckets) {
				Map<String, Bucket<KeyValueFrequency>> nextSourceMap = new HashMap<>();
				nextSourceMap.putAll(source.getBucketsCombination());
				BucketCombination nextSource = new BucketCombination(nextSourceMap);
				nextSource.getBucketsCombination().put(keyName, bucket);
				makeShardingTable(nextSource, restOfKeyNames);
			}
		}
	}
}
