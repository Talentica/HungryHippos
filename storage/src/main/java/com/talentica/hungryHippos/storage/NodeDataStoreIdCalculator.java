package com.talentica.hungryHippos.storage;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.utility.marshaling.DataDescription;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;

/**
 * Created by debasishc on 26/8/15.
 */
public class NodeDataStoreIdCalculator implements Serializable {

	private Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap = null;

	private static final long serialVersionUID = -4962284637100465382L;
	private Map<String, Set<Bucket<KeyValueFrequency>>> keyWiseAcceptingBuckets = new HashMap<>();
	private final String[] keys;
	private final DynamicMarshal dynamicMarshal;

	public NodeDataStoreIdCalculator(Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap,
			int thisNode, DataDescription dataDescription) {
		this.keyToValueToBucketMap = keyToValueToBucketMap;
		keys = dataDescription.keyOrder();
		for(String key:keyToValueToBucketMap.keySet()){
			Set<Bucket<KeyValueFrequency>> keyWiseBuckets = new HashSet<>();
			for(Object keyValue:keyToValueToBucketMap.get(key).keySet()){
				keyWiseBuckets.add(keyToValueToBucketMap.get(key).get(keyValue));
			}
			this.keyWiseAcceptingBuckets.put(key, keyWiseBuckets);
		}
		this.dynamicMarshal = new DynamicMarshal(dataDescription);
	}

	public int storeId(ByteBuffer row) {
		int fileId = 0;
		for (int i = keys.length - 1; i >= 0; i--) {
			fileId <<= 1;
			Object value=dynamicMarshal.readValue(i, row);
			Bucket<KeyValueFrequency> valueBucket = keyToValueToBucketMap.get(keys[i]).get(value);
			if (valueBucket != null && keyWiseAcceptingBuckets.get(keys[i]) != null
					&& keyWiseAcceptingBuckets.get(keys[i]).contains(valueBucket)) {
				fileId |= 1;
			}

		}
		return fileId;
	}
}
