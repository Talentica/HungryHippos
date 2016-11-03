/**
 * 
 */
package com.talentica.hungryHippos.coordination.listeners;

import com.talentica.hungryHippos.coordination.domain.Server;
import com.talentica.hungryHippos.coordination.domain.Status;

/**
 * {@code EvictionListener} used for deregistering a server.
 * 
 * @author PooshanS
 *
 */
public interface EvictionListener {

  /**
   * Deregister a server.
   * 
   * @param server
   * @return {@link Status}.
   */
  public Status deregister(Server server);

}
