/**
 * 
 */
package com.talentica.hungryHippos.utility.memory;

import java.util.HashMap;
import java.util.Map;

import com.talentica.hungryHippos.utility.CommonUtil;
import com.talentica.hungryHippos.utility.marshaling.FieldTypeArrayDataDescription;

/**
 * @author PooshanS
 *
 */
public class MemoryCalculator implements Memory{
	
	private Map<Integer,Long> jobIdMemoMap = new HashMap<>();
	private Map<Integer,Long> jobIdRowCountMap;
	private FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription();
	
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
		CommonUtil.setDataDescription(dataDescription);
		return (rowCount * dataDescription.getSize());
	}

}
