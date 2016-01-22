/**
 * 
 */
package com.talentica.hungryHippos.coordination.listeners;

import java.util.Set;

import com.talentica.hungryHippos.coordination.domain.Server;
import com.talentica.hungryHippos.coordination.domain.Status;

/**
 * @author PooshanS
 *
 */
public interface RegistrationListener {

    public Status register(Server server);

    public Set<Server> getRegisteredServers();

    public Status updateServer(Server server);

}
