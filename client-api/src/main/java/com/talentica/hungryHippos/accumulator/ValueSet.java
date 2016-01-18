package com.talentica.hungryHippos.accumulator;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by debasishc on 9/9/15.
 */
public class ValueSet implements Serializable{
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Object[] values;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

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

    public ValueSet(Object[] values) {
        this.values = values;
    }

    @Override
    public String toString() {
        return "ValueSet{" +
                "values=" + Arrays.toString(values) +
                '}';
    }
}
