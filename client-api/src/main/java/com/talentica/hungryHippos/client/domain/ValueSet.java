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

	private int encodedKeyIndexes;

	@SuppressWarnings("rawtypes")
	private Comparable[] values;

	@SuppressWarnings("rawtypes")
	public ValueSet(int[] keyIndexes, Comparable[] values) {
		this.encodedKeyIndexes = ArrayEncoder.encode(keyIndexes);
		setValues(values);
	}

	public ValueSet(int[] keyIndexes) {
		this.encodedKeyIndexes = ArrayEncoder.encode(keyIndexes);
		this.values = new Comparable[keyIndexes.length];
	}
	
	public ValueSet(int encodedKeyIndexex){
		this.encodedKeyIndexes = encodedKeyIndexex;
		this.values = new Comparable[ArrayEncoder.decode(encodedKeyIndexes).length];
	}
	
	public int[] getKeyIndexes(){
		return ArrayEncoder.decode(encodedKeyIndexes);
	}
	
	public int getEncodedKey(){
		return encodedKeyIndexes;
	} 
	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		ValueSet valueSet = (ValueSet) o;
		return (encodedKeyIndexes == valueSet.encodedKeyIndexes) & Arrays.equals(values, valueSet.values);
	}

	public Object[] getValues() {
		return values;
	}

	@SuppressWarnings("rawtypes")
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
		return encodedKeyIndexes;
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		int[] keyIndexes = ArrayEncoder.decode(encodedKeyIndexes);
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
			if (encodedKeyIndexes > otherValueSet.encodedKeyIndexes) {
				return 1;
			} else if (encodedKeyIndexes < otherValueSet.encodedKeyIndexes) {
				return -1;
			} else {
				for (int i = 0; i < values.length; i++) {
					if (values[i].equals(otherValueSet.values[i])) {
						continue;
					}
					return values[i].compareTo(otherValueSet.values[i]);
				}
			}
		}
		return 0;
	}
}
