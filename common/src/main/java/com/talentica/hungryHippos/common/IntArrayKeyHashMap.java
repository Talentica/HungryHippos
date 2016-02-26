/**
 * 
 */
package com.talentica.hungryHippos.common;

import java.util.Arrays;

/**
 * @author PooshanS
 *
 */
public class IntArrayKeyHashMap {

	private final int[] values;

    public IntArrayKeyHashMap(int[] values) {
        this.values = values;
    }
    
    public int[] getValues(){
    	return values;
    }

    @Override
    public boolean equals(Object another) {
        if (another == this) {
            return true;
        }
        if (another == null) {
            return false;
        }
        if (another.getClass() != this.getClass()) {
            return false;
        }
        IntArrayKeyHashMap key = (IntArrayKeyHashMap) another;
        return Arrays.equals(this.values, key.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(this.values);
    }
    
    @Override
    public String toString() {
		return Arrays.toString(values);
    }
    
	
	
	
}
