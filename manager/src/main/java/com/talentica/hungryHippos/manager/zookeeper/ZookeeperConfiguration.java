/**
 * 
 */
package com.talentica.hungryHippos.manager.zookeeper;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author PooshanS
 *
 */
public class ZookeeperConfiguration {

    Integer sessionTimeout;
    Map<String,String> pathMap;
    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperConfiguration.class);

    /**
     * @param hosts
     * @param basePath
     * @param alertsPath
     * @param sessionTimeout
     * @param configPath
     */
    public ZookeeperConfiguration(Map<String,String> pathMap,Integer sessionTimeout){
    	this.pathMap = pathMap;
    	this.sessionTimeout = sessionTimeout;
    }

    public Integer getSessionTimeout() {
        return sessionTimeout;
    }

    public void setSessionTimeout(Integer sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

	public Map<String, String> getPathMap() {
		return pathMap;
	}

	public void setPathMap(Map<String, String> pathMap) {
		this.pathMap = pathMap;
	}


}
