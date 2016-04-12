package com.talentica.hungryHippos.storage;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;

import com.talentica.hungryHippos.sharding.BucketUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.utility.MapUtils;

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
			String key = keys[i];
			int keyIndex = Integer.parseInt(key.substring(3)) - 1 ;
			Object value = dynamicMarshal.readValue(keyIndex, row);
			Map<Object, Bucket<KeyValueFrequency>> valueToBucketMap = keyToValueToBucketMap.get(keys[i]);
			Bucket<KeyValueFrequency> valueBucket = valueToBucketMap.get(value);
			if(valueBucket == null){
				Collection<Bucket<KeyValueFrequency>> keyBuckets = (Collection<Bucket<KeyValueFrequency>>) valueToBucketMap.values();
				List<Bucket<KeyValueFrequency>> keyBucketList = new ArrayList(keyBuckets);
				Collections.sort(keyBucketList);
				int bucketNo = value.hashCode() % keyBucketList.get(keyBucketList.size() - 1).getId();
				valueBucket = BucketUtil.getBucket(valueToBucketMap, bucketNo);
			}
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
