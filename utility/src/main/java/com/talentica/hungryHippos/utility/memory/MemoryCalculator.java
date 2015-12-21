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
	private Map<Integer,Integer> jobIdRowCountMap;
	private FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription();
	
	public MemoryCalculator(Map<Integer,Integer> jobIdRowCountMap){
		this.jobIdRowCountMap = jobIdRowCountMap;
	}
	
	@Override
	public Map<Integer, Long> getJobMemoryAlloc() {
		for(Map.Entry<Integer, Integer> e : jobIdRowCountMap.entrySet()){
			jobIdMemoMap.put(e.getKey(), getObjectSize(e.getValue()));
		}
		return jobIdMemoMap;
	}

	private long getObjectSize(int rowCount) {
		CommonUtil.setDataDescription(dataDescription);
		return (rowCount * dataDescription.getSize());
	}

}
