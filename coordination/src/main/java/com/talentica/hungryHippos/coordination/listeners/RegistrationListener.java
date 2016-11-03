/**
 * 
 */
package com.talentica.hungryHippos.coordination.listeners;

import java.util.Set;

import com.talentica.hungryHippos.coordination.domain.Server;
import com.talentica.hungryHippos.coordination.domain.Status;

/**
 * {@code RegistrationListener} used for registering {@link Server}.
 * 
 * @author PooshanS
 *
 */
public interface RegistrationListener {

  /**
   * register a server.
   * 
   * @param server
   * @return a {@link Status} specifying the registration was successful or not.
   */
  public Status register(Server server);

  /**
   * retrieves the server's that are registered.
   * 
   * @return {@link Set}.
   */
  public Set<Server> getRegisteredServers();

  /**
   * update a Server details.
   * 
   * @param server
   * @return {@link Status}
   */
  public Status updateServer(Server server);

}
