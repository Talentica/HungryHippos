/**
 * 
 */
package com.talentica.hungryHippos.utility;


/**
 * @author PooshanS
 *
 */
public class MemoryStatus{

	
	/**
	 * Return memory in MB
	 * 
	 * @return long
	 */
	public static long getFreeMemory() {
		return Runtime.getRuntime().freeMemory()/(1024*1024);
	}

	/**
	 * Return memory in MB
	 * 
	 * @return long
	 */
	public static long getMaxMemory() {
		return Runtime.getRuntime().maxMemory()/(1024*1024);
	}

	/**
	 * Return memory in MB
	 * 
	 * @return long
	 */
	public static long getTotalmemory() {
		return Runtime.getRuntime().totalMemory()/(1024*1024);
	}
	
}
