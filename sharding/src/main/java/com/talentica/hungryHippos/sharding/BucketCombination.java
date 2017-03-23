/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
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
