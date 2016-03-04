package com.talentica.hungryHippos.client.domain;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by debasishc on 9/9/15.
 */
public class ValueSet implements Comparable<ValueSet>, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private int[] keyIndexes;

	private Comparable[] values;

	public ValueSet(int[] keyIndexes, Comparable[] values) {
		this.keyIndexes = keyIndexes;
		setValues(values);
	}

	public ValueSet(int[] keyIndexes) {
		this.keyIndexes = keyIndexes;
		this.values = new Comparable[keyIndexes.length];
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

	public void setValues(Comparable[] values) {
		this.values = values;
		if (values != null) {
			for (int i = 0; i < values.length; i++) {
				setValue(values[i], i);
			}
		}
	}

	public void setValue(Object value, int index) {
		if (value instanceof MutableCharArrayString) {
			this.values[index] = ((MutableCharArrayString) value).clone();
		}
	}

	@Override
	public int hashCode() {
		int h = 0;
		int off = 0;
		for (int i = 0; i < keyIndexes.length; i++) {
			h = 31 * h + keyIndexes[off];
			h = h + values[off].hashCode();
			off++;
		}
		return h;
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

	@SuppressWarnings("unchecked")
	@Override
	public int compareTo(ValueSet otherValueSet) {
		if (equals(otherValueSet)) {
			return 0;
		}
		if (values != null && otherValueSet.values != null) {
			if (values.length != otherValueSet.values.length) {
				return values.length - otherValueSet.values.length;
			}

			for (int i = 0; i < values.length; i++) {
				if (values[i].equals(otherValueSet.values[i])) {
					continue;
				}
				return values[i].compareTo(otherValueSet.values[i]);
			}
		}
		return 0;
	}
}
