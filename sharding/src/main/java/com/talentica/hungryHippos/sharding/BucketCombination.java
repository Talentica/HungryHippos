package com.talentica.hungryHippos.sharding;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.talentica.hungryHippos.coordination.annotations.ZkTransient;

/**
 * {@code BucketCombination } used for creating combination of buckets that will be sent to a particylar node.
 * @author debasishc 
 * @since 14/8/15.
 */
public class BucketCombination implements Serializable {
	/**
	 * 
	 */
  @ZkTransient
	private static final long serialVersionUID = 3581984005135868712L;
	private Map<String, Bucket<KeyValueFrequency>> bucketsCombination;

	public BucketCombination(Map<String, Bucket<KeyValueFrequency>> keyValueCombination) {
		this.bucketsCombination = keyValueCombination;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || !(o instanceof BucketCombination)) {
			return false;
		}
		BucketCombination that = (BucketCombination) o;
		return bucketsCombination.equals(that.bucketsCombination);

	}

	public boolean checkMatchAnd(BucketCombination rhs) {
		for (String k : rhs.bucketsCombination.keySet()) {
			Object thatValue = rhs.bucketsCombination.get(k);
			if (!bucketsCombination.containsKey(k)) {
				continue;
			} else {
				Object thisValue = bucketsCombination.get(k);
				if (thisValue == null) {
					continue;
				} else {
					if (thisValue.equals(thatValue)) {
						continue;
					} else {
						return false;
					}
				}
			}
		}
		return true;
	}

	/**
	 * checks whether BucketCombination is same.
	 * @param rhs
	 * @return
	 */
	public boolean checkMatchOr(BucketCombination rhs) {
		for (String k : rhs.bucketsCombination.keySet()) {
			Object thatValue = rhs.bucketsCombination.get(k);
			if (!bucketsCombination.containsKey(k)) {
				continue;
			} else {
				Object thisValue = bucketsCombination.get(k);
				if (thisValue == null) {
					continue;
				} else {
					if (thisValue.equals(thatValue)) {
						return true;
					} else {
						continue;
					}
				}
			}
		}
		return false;
	}

	/**
	 * checks whether BucjetCombination is same.
	 * @param rhs
	 * @return
	 */
	public boolean checkMatchOr(List<BucketCombination> rhs) {
		for (BucketCombination k : rhs) {
			if (this.checkMatchOr(k)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public int hashCode() {
		return bucketsCombination != null ? bucketsCombination.toString().hashCode() : 0;
	}

	public Map<String, Bucket<KeyValueFrequency>> getBucketsCombination() {
		return bucketsCombination;
	}

	public void setKeyValueCombination(Map<String, Bucket<KeyValueFrequency>> keyValueCombination) {
		this.bucketsCombination = keyValueCombination;
	}

	@Override
	public String toString() {
		return "BucketCombination{" + bucketsCombination + '}';
	}
}
