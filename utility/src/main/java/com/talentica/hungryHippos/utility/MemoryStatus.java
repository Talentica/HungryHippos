/**
 * 
 */
package com.talentica.hungryHippos.utility;


/**
 * @author PooshanS
 *
 */
public class MemoryStatus{

	
	private static final int _1MB = 1024 * 1024;

	private static final long MAX_MEMORY = Runtime.getRuntime().maxMemory() / _1MB;

	/**
	 * Return memory in MB
	 * 
	 * @return long
	 */
	public static long getFreeMemory() {
		return Runtime.getRuntime().freeMemory()/_1MB;
	}
	
	public static long getMaximumFreeMemoryThatCanBeAllocated() {
		return (getMaxMemory() - getUsedMemory());
	}

	public static long getUsedMemory() {
		return (getTotalmemory() - getFreeMemory());
	}

	/**
	 * Return memory in MB
	 * 
	 * @return long
	 */
	public static long getMaxMemory() {
		return MAX_MEMORY;
	}

	/**
	 * Return memory in MB
	 * 
	 * @return long
	 */
	public static long getTotalmemory() {
		return Runtime.getRuntime().totalMemory()/_1MB;
	}
	
}
