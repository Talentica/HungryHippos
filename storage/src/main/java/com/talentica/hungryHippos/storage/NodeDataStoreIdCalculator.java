package com.talentica.hungryHippos.storage;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.utility.MapUtils;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;

/**
 * Created by debasishc on 26/8/15.
 */
public class NodeDataStoreIdCalculator implements Serializable {

	private static final long serialVersionUID = -4962284637100465382L;
	private Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap = null;
	private Map<String, Set<Bucket<KeyValueFrequency>>> keyWiseAcceptingBuckets = new HashMap<>();
	private final String[] keys;
	private final DynamicMarshal dynamicMarshal;
	private int count = 0;
	private Logger LOGGER = LoggerFactory.getLogger(NodeDataStoreIdCalculator.class);

	public NodeDataStoreIdCalculator(Map<String, Map<Object, Bucket<KeyValueFrequency>>> keyToValueToBucketMap,
			Map<String, Map<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap,
			int thisNode,
			DataDescription dataDescription) {
		this.keyToValueToBucketMap = keyToValueToBucketMap;
		keys = dataDescription.keyOrder();
		setKeyWiseAcceptingBuckets(bucketToNodeNumberMap, thisNode);
		this.dynamicMarshal = new DynamicMarshal(dataDescription);
	}

	private void setKeyWiseAcceptingBuckets(Map<String, Map<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap,
			int thisNode) {
		for (String key : bucketToNodeNumberMap.keySet()) {
			Set<Bucket<KeyValueFrequency>> keyWiseBuckets = new HashSet<>();
			Map<Bucket<KeyValueFrequency>, Node> bucketToNodeMap = bucketToNodeNumberMap.get(key);
			for (Bucket<KeyValueFrequency> bucket : bucketToNodeMap.keySet()) {
				if (bucketToNodeMap.get(bucket).getNodeId() == thisNode) {
					keyWiseBuckets.add(bucket);
				}
			}
			this.keyWiseAcceptingBuckets.put(key, keyWiseBuckets);
		}
		LOGGER.info("keyWiseAcceptingBuckets:{}", MapUtils.getFormattedString(keyWiseAcceptingBuckets));
	}

	public int storeId(ByteBuffer row) {
		int fileId = 0;
		for (int i = keys.length - 1; i >= 0; i--) {
			fileId <<= 1;
			Object value = dynamicMarshal.readValue(i, row);
			Bucket<KeyValueFrequency> valueBucket = keyToValueToBucketMap.get(keys[i]).get(value);
			if (valueBucket != null && keyWiseAcceptingBuckets.get(keys[i]) != null
					&& keyWiseAcceptingBuckets.get(keys[i]).contains(valueBucket)) {
				fileId |= 1;
			}
		}
		return fileId;
	}

	public int getCount() {
		return count;
	}
}
