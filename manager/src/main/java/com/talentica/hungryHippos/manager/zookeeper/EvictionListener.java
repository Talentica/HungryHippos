/**
 * 
 */
package com.talentica.hungryHippos.manager.zookeeper;

/**
 * @author PooshanS
 *
 */
public interface EvictionListener {

	 public Status deregister(Server server);
	 
}
