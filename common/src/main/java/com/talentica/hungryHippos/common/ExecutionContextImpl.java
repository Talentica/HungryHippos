package com.talentica.hungryHippos.common;

import java.io.File;
import java.io.PrintStream;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.ExecutionContext;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.client.domain.ValueSet;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;

/**
 * Created by debasishc on 9/9/15.
 */
public class ExecutionContextImpl implements ExecutionContext {
	private ByteBuffer data;
	private final DynamicMarshal dynamicMarshal;
	private ValueSet keys;
	private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionContextImpl.class);

	private static PrintStream out;

	static {
		try {
			out = new PrintStream(
					new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath() + PathUtil.FORWARD_SLASH + "outputFile");
		} catch (Exception ex) {
			LOGGER.error("Exception occurred while getting print stream for outoutFile.", ex);
		}
	}

	public ExecutionContextImpl(DynamicMarshal dynamicMarshal) {
		this.dynamicMarshal = dynamicMarshal;
	}

	public void setData(ByteBuffer data) {
		this.data = data;
	}

	@Override
	public ByteBuffer getData() {
		return data;
	}

	@Override
	public Object getValue(int index) {
		return dynamicMarshal.readValue(index, data);
	}

	@Override
	public MutableCharArrayString getString(int index) {
		return dynamicMarshal.readValueString(index, data);
	}

	@Override
	public void saveValue(int calculationIndex, Object value) {
		out.println("Calculation result for column index " + calculationIndex + " and " + keys.toString() + " is ====>"
				+ value);
	}

	@Override
	public void saveValue(int calculationIndex, Object value, String metric) {
		out.println(metric + " metric calculation result for column index " + calculationIndex + " and "
				+ keys.toString() + " is ====>" + value);
	}

	@Override
	public void setKeys(ValueSet valueSet) {
		this.keys = valueSet;
	}

	@Override
	public ValueSet getKeys() {
		return keys;
	}
}
