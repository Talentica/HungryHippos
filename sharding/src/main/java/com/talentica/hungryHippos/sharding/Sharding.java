package com.talentica.hungryHippos.sharding;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.utility.CommonUtil;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.marshaling.MutableCharArrayString;
import com.talentica.hungryHippos.utility.marshaling.Reader;

/**
 * Created by debasishc on 14/8/15.
 */
public class Sharding {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(Sharding.class);

	// Map<key1,{KeyValueFrequency(value1,10),KeyValueFrequency(value2,11)}>
	private Map<String, List<KeyValueFrequency>> keyValueFrequencyMap = new HashMap<>();

	// Map<Key1,Map<value1,Node(1)>
	private Map<String, Map<Object, Node>> keyValueNodeNumberMap = new HashMap<>();
	PriorityQueue<Node> fillupQueue = new PriorityQueue<>(new NodeRemainingCapacityComparator());

	// e.g. Map<KeyCombination({key1,value1},{key2,value2},{key3,value3}),count>
	private Map<KeyCombination, Long> keyCombinationFrequencyMap = new HashMap<>();
	private Map<Node, List<KeyCombination>> nodeToKeyMap = new HashMap<>();
	private Map<KeyCombination, Set<Node>> keyCombinationNodeMap = new HashMap<>();
	private final static String keyValueNodeNumberMapFile = "keyValueNodeNumberMap";
	private final static String keyCombinationNodeMapFile = "keyCombinationNodeMap";


	public Sharding(int numNodes) {
		for (int i = 0; i < numNodes; i++) {
			Node node = new Node(300000, i);
			fillupQueue.offer(node);
			nodeToKeyMap.put(node, new ArrayList<KeyCombination>());
		}
	}

	public static void doSharding(Reader input) {
		LOGGER.info("SHARDING STARTED");
		Sharding sharding = new Sharding(Property.getTotalNumberOfNodes());
		try {
			sharding.populateFrequencyFromData(input);
			sharding.shardAllKeys();
			System.out.println(sharding.keyCombinationNodeMap.size());
			
			CommonUtil.dumpFileOnDisk(Sharding.keyCombinationNodeMapFile, sharding.keyCombinationNodeMap);
			CommonUtil.dumpFileOnDisk(Sharding.keyValueNodeNumberMapFile, sharding.keyValueNodeNumberMap);
			
		} catch (IOException | NodeOverflowException e) {
			e.printStackTrace();
			LOGGER.error("Error occurred during sharding process.", e);
		} 
	}

	// TODO: This method needs to be generalized
	Map<String, List<KeyValueFrequency>> populateFrequencyFromData(Reader data) throws IOException {
		String[] keys = { "key1", "key2", "key3" };
		// Map<key1,Map<value1,count>>
		Map<String, Map<Object, Long>> keyValueFrequencyMap = new HashMap<>();
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

			Map<String, Object> keyCombinationMap = new HashMap<>();
			for (int i = 0; i < keys.length; i++) {
				keyCombinationMap.put(keys[i], values[i]);
			}

			KeyCombination keyCombination = new KeyCombination(keyCombinationMap);

			Long count = keyCombinationFrequencyMap.get(keyCombination);
			if (count == null) {
				keyCombinationFrequencyMap.put(keyCombination, 1L);
			} else {
				keyCombinationFrequencyMap.put(keyCombination, count + 1);
			}

			for (int i = 0; i < keys.length; i++) {
				Map<Object, Long> frequencyPerValue = keyValueFrequencyMap.get(keys[i]);
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
		for (int i = 0; i < keys.length; i++) {
			Map<Object, Long> frequencyPerValue = keyValueFrequencyMap.get(keys[i]);
			List<KeyValueFrequency> freqList = new ArrayList<>();
			this.keyValueFrequencyMap.put(keys[i], freqList);
			for (Map.Entry<Object, Long> fv : frequencyPerValue.entrySet()) {
				freqList.add(new KeyValueFrequency(fv.getKey(), fv.getValue()));
			}
		}
		return this.keyValueFrequencyMap;
	}

	private void shardSingleKey(String keyName) throws NodeOverflowException {
		List<KeyValueFrequency> keyValueFrequencies = keyValueFrequencyMap.get(keyName);
		Map<Object, Node> keyValueNodeNumber = new HashMap<>();
		keyValueNodeNumberMap.put(keyName, keyValueNodeNumber);
		Collections.sort(keyValueFrequencies);
		for (KeyValueFrequency kvf : keyValueFrequencies) {
			Node mostEmptyNode = fillupQueue.poll();

			List<KeyCombination> currentKeys = nodeToKeyMap.get(mostEmptyNode);
			if (currentKeys == null) {
				currentKeys = new ArrayList<>();
				nodeToKeyMap.put(mostEmptyNode, currentKeys);
			}
			long currentSize = sumForKeyCombinationUnion(currentKeys);

			Map<String, Object> wouldBeMap = new HashMap<>();
			wouldBeMap.put(keyName, kvf.getKeyValue());
			currentKeys.add(new KeyCombination(wouldBeMap));
			long wouldBeSize = sumForKeyCombinationUnion(currentKeys);

			mostEmptyNode.fillUpBy(wouldBeSize - currentSize);

			fillupQueue.offer(mostEmptyNode);
			keyValueNodeNumber.put(kvf.getKeyValue(), mostEmptyNode);
		}
	}

	private long sumForKeyCombinationUnion(List<KeyCombination> keyCombination) {
		long sum = 0;
		for (Map.Entry<KeyCombination, Long> entry : keyCombinationFrequencyMap.entrySet()) {
			KeyCombination keyCombination1 = entry.getKey();
			if (keyCombination1.checkMatchOr(keyCombination)) {
				sum += entry.getValue();
			}
		}
		return sum;
	}

	public void shardAllKeys() throws NodeOverflowException {
		for (String key : keyValueFrequencyMap.keySet()) {
			LOGGER.info("Sharding on key: {}", key);
			shardSingleKey(key);
		}
		List<String> keyNameList = new ArrayList<>();
		keyNameList.addAll(keyValueFrequencyMap.keySet());
		makeShardingTable(new KeyCombination(new HashMap<String, Object>()), keyNameList);
	}

	private void makeShardingTable(KeyCombination source, List<String> keyNames) throws NodeOverflowException {
		String keyName;
		if (keyNames.size() >= 1) {
			keyName = keyNames.get(0);
		} else {
			keyName = null;
		}
		if (keyName == null) {
			// exit case
			// lets check which nodes it goes.
			Set<Node> nodesForKeyCombination = new HashSet<>();
			for (Map.Entry<String, Object> kv : source.getKeyValueCombination().entrySet()) {
				Node n = keyValueNodeNumberMap.get(kv.getKey()).get(kv.getValue());
				if (n != null) {
					nodesForKeyCombination.add(n);
				}
			}
			int numberOfIntersectionStorage = source.getKeyValueCombination().size() - nodesForKeyCombination.size();
			Set<Node> nodesToPutBack = new HashSet<>();
			while (numberOfIntersectionStorage > 0) {
				Node mostEmptyNode = fillupQueue.poll();
				if (!nodesForKeyCombination.contains(mostEmptyNode)) {
					nodesForKeyCombination.add(mostEmptyNode);
					Long value = keyCombinationFrequencyMap.get(source);
					if (value == null) {
						value = 0l;
					}
					mostEmptyNode.fillUpBy(value);
					numberOfIntersectionStorage--;
				}
				nodesToPutBack.add(mostEmptyNode);
			}
			nodesToPutBack.forEach(n -> fillupQueue.offer(n));
			keyCombinationNodeMap.put(source, nodesForKeyCombination);

		} else {
			List<String> restOfKeyNames = new LinkedList<>();
			restOfKeyNames.addAll(keyNames);
			restOfKeyNames.remove(keyName);
			List<KeyValueFrequency> kvfs = keyValueFrequencyMap.get(keyName);

			for (KeyValueFrequency keyValueFrequency : kvfs) {
				Object value = keyValueFrequency.getKeyValue();
				Map<String, Object> nextSourceMap = new HashMap<>();
				nextSourceMap.putAll(source.getKeyValueCombination());
				KeyCombination nextSource = new KeyCombination(nextSourceMap);
				nextSource.getKeyValueCombination().put(keyName, value);
				makeShardingTable(nextSource, restOfKeyNames);
			}
		}
	}

}
