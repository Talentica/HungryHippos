/**
 * 
 */
package com.talentica.hungryHippos.utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Memory optimization by allocation of the four character in the four bytes.
 * 
 * @author PooshanS
 *
 */
public class CharUtil {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(CharUtil.class.getName());
	private static int bufferSize = Integer.SIZE;;
	private static int totalByte = Integer.BYTES;
	
	/**
	 * It return the resultant value corresponding to the all ASCII value positioning each ASCII values
	 * to bit's position from right 
	 * 
	 * @param ch char arrays
	 * @param bufferSize 32 bits
	 * @return int value
	 */
	public static int getIntValue(char[] ch){
		if(ch.length > totalByte){
			LOGGER.warn("Char arrays having length more than buffer size {} bytes ",totalByte);
			return 0;
		}
		int bitsPerByte = 8;
		int noOfBits = bufferSize;
		int oldValue = 0;
		
		for(int i = 0 ; i < ch.length ; i++){
			int ascii = ch[i];
			int newValue = ascii << (noOfBits - (bitsPerByte * (i+1)));
			oldValue = oldValue | newValue;
		}
		return oldValue;
	}
	
	/**
	 * It returns the all char in arrays corresponding to the bits representation of the given numbers.
	 * 
	 * @param value
	 * @param bufferSize 32 bits
	 * @return char[] arrays of chars
	 */
	public static char[] getCharArrays(int value){
		int bitsPerByte = 8;
		int oldValue = value;
		int noOfBytes = bufferSize / bitsPerByte ;
		char[] chs = new char[noOfBytes] ;
		
		for(int i = (noOfBytes-1) ; i >= 0  ; i--){
			int tempValue = oldValue;
			int newValue = (tempValue >> (bitsPerByte * i)) & 0x000000FF;
			chs[noOfBytes - i - 1] = (char)newValue;
		}
		return chs;
	}
}
