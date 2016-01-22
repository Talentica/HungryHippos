/**
 * 
 */
package com.talentica.hungryHippos.coordination.listeners;

import com.talentica.hungryHippos.coordination.domain.Server;
import com.talentica.hungryHippos.coordination.domain.Status;

/**
 * @author PooshanS
 *
 */
public interface EvictionListener {

	 public Status deregister(Server server);
	 
}
