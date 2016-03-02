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
	
	public static long getMaximumFreeMemoryThatCanBeAllocated() {
		return getMaxMemory() - (getTotalmemory() - getFreeMemory());
	}

	/**
	 * Return memory in MB
	 * 
	 * @return long
	 */
	public static long getMaxMemory() {
		return Long.parseLong(Property.getPropertyValue("node.max.memory.in.mbs"));
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
