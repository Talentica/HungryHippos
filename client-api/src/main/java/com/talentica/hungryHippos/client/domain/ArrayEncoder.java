/**
 * 
 */
package com.talentica.hungryHippos.client.domain;

import java.util.ArrayList;
import java.util.List;

/**
 * @author PooshanS
 *
 */
public class ArrayEncoder {
	private static List<Integer> decodedValue = new ArrayList<Integer>();
	
	public static int encode(int[] keys) {
		int encodedValue = 0;
		for (int i = 0; i < keys.length; i++) {
			encodedValue = encodedValue | 1 << keys[i];
		}
		return encodedValue;
	}
	
	public static int[] decode(int encodedValue){
		decodedValue.clear();
		int mask = 1;
		int index = 0;
		do{
			if((encodedValue & mask) != 0){
				decodedValue.add(index);
			}
			index++;
			encodedValue = encodedValue>>1;
		}while(encodedValue!=0);
		return decodedValue.stream().mapToInt(i->i).toArray();
	}

}
