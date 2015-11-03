/**
 * 
 */
package com.talentica.hungryHippos.manager.zookeeper;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author PooshanS
 *
 */
public class AlertManager implements EvictionListener, RegistrationListener{
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AlertManager.class.getName());
	Map<Server, Boolean> concurrentMap = new ConcurrentHashMap<Server, Boolean>(new TreeMap<Server, Boolean>());
    Set<Server> registeredServers = Collections.newSetFromMap(concurrentMap);
  
    @Override
    public synchronized Status deregister(Server server) {
        if (registeredServers.remove(server)) {
            return Status.createStatus(String.format("Server %s Deregistered", server.getName()));
        } else {
           LOGGER.info(String.format("Attempt to remove non-monitored server: %s [%s] :: %s",
                    server.getName(), server.getServerAddress().getIp(), server.getDescription()));
            return Status.createErrorStatus(String.format("Server %s was not a registered " +
                    "server", server.getName()));
        }
    }

    @Override
    public Status register(Server server) {
        if (registeredServers.add(server)) {        	
            return Status.createStatus(String.format("Server %s added", server.getName()));
        } else {
            return Status.createErrorStatus(String.format("Server %s was already registered",
                    server.getName()));
        }
    }

    @Override
    public Set<Server> getRegisteredServers() {
        return registeredServers;
    }

    @Override
    public Status updateServer(Server server) {
        if (registeredServers.remove(server)) {
            registeredServers.add(server);
            return Status.createStatus("Server " + server.getName() + " updated");
        } else {
            return Status.createErrorStatus("Server " + server + " was not registered");
        }
    }

    /*private void tryOwnAlert(final Server unregisteredServer) {

    }*/


}
