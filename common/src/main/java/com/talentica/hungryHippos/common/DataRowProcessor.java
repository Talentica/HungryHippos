package com.talentica.hungryHippos.common;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.ValueSet;
import com.talentica.hungryHippos.client.domain.Work;
import com.talentica.hungryHippos.storage.RowProcessor;
import com.talentica.hungryHippos.utility.JobEntity;
import com.talentica.hungryHippos.utility.MemoryStatus;
import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;

/**
 * Created by debasishc on 9/9/15.
 */
public class DataRowProcessor implements RowProcessor {

	private DynamicMarshal dynamicMarshal;

	private ExecutionContextImpl executionContext;

	private JobEntity jobEntity;

	private TreeMap<ValueSet, List<Work>> valuestWorkTree = new TreeMap<>();

	private int[] keys;

	Object[] values = null;
	
	private static final long AVAILABLE_RAM = Long.valueOf(Property.getPropertyValue("node.available.ram"));
	
	private static ValueSet maxValueSet;
	
	long startTime = System.currentTimeMillis();
	private static final Logger LOGGER = LoggerFactory.getLogger(DataRowProcessor.class.getName());

	public DataRowProcessor(DynamicMarshal dynamicMarshal, JobEntity jobEntity) {
		this.jobEntity = jobEntity;
		this.dynamicMarshal = dynamicMarshal;
		this.keys = jobEntity.getJob().getDimensions();
		this.executionContext = new ExecutionContextImpl(dynamicMarshal);
		values = new Object[keys.length];
	}

	@Override
	public void processRow(ByteBuffer row) {
		List<Work> works = null;
		ValueSet valueSet = new ValueSet(keys);
		for (int i = 0; i < keys.length; i++) {
			Object value = dynamicMarshal.readValue(keys[i], row);
			valueSet.setValue(value, i);
		}		
		
		if (MemoryStatus.getFreeMemory() > AVAILABLE_RAM) {
			works = valuestWorkTree.get(valueSet);
			if (works == null) {
				works = new ArrayList<>();
				Work work = jobEntity.getJob().createNewWork();
				works.add(work);
				valuestWorkTree.put(valueSet, works);
			}
			maxValueSet = valuestWorkTree.lastKey();
		} else if(valueSet.compareTo(maxValueSet) > 0){
				works = valuestWorkTree.remove(maxValueSet);
				works.clear();
				Work work = jobEntity.getJob().createNewWork();
				works.add(work);
				valuestWorkTree.put(valueSet, works);
				maxValueSet = valueSet;
		} 
		if(works == null) return;
		executionContext.setData(row);
		for (Work work : works) {
			work.processRow(executionContext);
		}
	}

	@Override
	public void finishUp() {
		for (Entry<ValueSet, List<Work>> e : valuestWorkTree.entrySet()) {
			for (Work work : e.getValue()) {
				executionContext.setKeys(e.getKey());
				work.calculate(executionContext);
			}
		}
	}

}