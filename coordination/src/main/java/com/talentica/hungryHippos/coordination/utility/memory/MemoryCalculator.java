/**
 * 
 */
package com.talentica.hungryHippos.coordination.utility.memory;

import java.util.HashMap;
import java.util.Map;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;

/**
 * @author PooshanS
 *
 */
public class MemoryCalculator implements Memory{
	
	private Map<Integer,Long> jobIdMemoMap = new HashMap<>();
	private Map<Integer,Long> jobIdRowCountMap;
	
	public MemoryCalculator(Map<Integer,Long> jobIdRowCountMap){
		this.jobIdRowCountMap = jobIdRowCountMap;
	}
	
	@Override
	public Map<Integer, Long> getJobMemoryAlloc() {
		for(Map.Entry<Integer, Long> e : jobIdRowCountMap.entrySet()){
			jobIdMemoMap.put(e.getKey(), getObjectSize(e.getValue()));
		}
		return jobIdMemoMap;
	}

	private long getObjectSize(Long rowCount) {
		DataDescription dataDescription = CommonUtil.getConfiguredDataDescription();
		return (rowCount * dataDescription.getSize());
	}

}
