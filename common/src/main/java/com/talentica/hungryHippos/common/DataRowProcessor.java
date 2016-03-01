package com.talentica.hungryHippos.common;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.ValueSet;
import com.talentica.hungryHippos.client.domain.Work;
import com.talentica.hungryHippos.storage.RowProcessor;
import com.talentica.hungryHippos.utility.JobEntity;
import com.talentica.hungryHippos.utility.MemoryStatus;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;

/**
 * Created by debasishc on 9/9/15.
 */
public class DataRowProcessor implements RowProcessor {

	private DynamicMarshal dynamicMarshal;

	private ExecutionContextImpl executionContext;

	private HashMap<ValueSet, Work> valueSetWorksMap = new HashMap<ValueSet, Work>();

	private HashMap<ValueSet, TaskEntity> valueSetTaskEntityMap = new HashMap<>();

	private JobEntity jobEntity;

	private int[] keys;

	Object[] values = null;
	
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
		if ((System.currentTimeMillis() - startTime) / (1000 * 60) == 1) {
			LOGGER.info("FREE SPACE AVAILABLE {} MB", MemoryStatus.getFreeMemory());
			LOGGER.info("MAX SPACE AVAILABLE {} MB", MemoryStatus.getMaxMemory());
			LOGGER.info("TOTAL SPACE AVAILABLE {} MB", MemoryStatus.getTotalmemory());
			startTime = System.currentTimeMillis();
		}
		ValueSet valueSet = new ValueSet(keys);
		for (int i = 0; i < keys.length; i++) {
			Object value = dynamicMarshal.readValue(keys[i], row);
			valueSet.setValue(value, i);
		}
		Work work = valueSetWorksMap.get(valueSet);
		if (work == null) {
			work = jobEntity.getJob().createNewWork();
			valueSetWorksMap.put(valueSet, work);
		}
		executionContext.setData(row);
		work.processRow(executionContext);
	}

	@Override
	public void finishUp() {
		for (Map.Entry<ValueSet, Work> e : valueSetWorksMap.entrySet()) {
			executionContext.setKeys(e.getKey());
			e.getValue().calculate(executionContext);
		}
	}

}