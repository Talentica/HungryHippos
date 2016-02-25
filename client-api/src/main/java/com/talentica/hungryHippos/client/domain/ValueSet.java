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

	private int[] keyIndexes;

	private Object[] values;

	public ValueSet(int[] keyIndexes, Object[] values) {
		this.keyIndexes = keyIndexes;
		setValues(values);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		ValueSet valueSet = (ValueSet) o;
		// Probably incorrect - comparing Object[] arrays with Arrays.equals
		return Arrays.equals(values, valueSet.values) & Arrays.equals(keyIndexes, valueSet.keyIndexes);
	}

	public Object[] getValues() {
		return values;
	}

	public void setValues(Object[] values) {
		this.values = values;
		if (values != null) {
			for (int i = 0; i < values.length; i++) {
				if (values[i] instanceof MutableCharArrayString) {
					this.values[i] = ((MutableCharArrayString) values[i]).clone();
				}
			}
		}

	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(values);
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append("ValueSet{");
		if (keyIndexes != null && values != null && keyIndexes.length == values.length) {
			for (int count = 0; count < keyIndexes.length; count++) {
				if (count != 0) {
					result.append(",");
				}
				result.append(keyIndexes[count] + "=" + values[count]);
			}
		}
		result.append("}");
		return result.toString();
	}
}
