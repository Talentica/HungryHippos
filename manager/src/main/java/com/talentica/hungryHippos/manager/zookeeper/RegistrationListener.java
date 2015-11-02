/**
 * 
 */
package com.talentica.hungryHippos.manager.zookeeper;

import java.util.Set;

/**
 * @author PooshanS
 *
 */
public interface RegistrationListener {

    public Status register(Server server);

    public Set<Server> getRegisteredServers();

    public Status updateServer(Server server);

}
