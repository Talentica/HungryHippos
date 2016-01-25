package com.talentica.hungryHippos.client.domain;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by debasishc on 9/9/15.
 */
public class ValueSet implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String[] keys;

	private Object[] values;

	public ValueSet(String[] keys, Object[] values) {
		this.keys = keys;
		this.values = values;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		ValueSet valueSet = (ValueSet) o;

		// Probably incorrect - comparing Object[] arrays with Arrays.equals
		return Arrays.equals(values, valueSet.values);

	}

	public Object[] getValues() {
		return values;
	}

	public void setValues(Object[] values) {
		this.values = values;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(values);
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append("ValueSet{");
		if (keys != null && values != null && keys.length == values.length) {
			for (int count = 0; count < keys.length; count++) {
				if (count != 0) {
					result.append(",");
				}
				result.append(keys[count] + "=" + values[count]);
			}
		}
		result.append("}");
		return result.toString();
	}
}
