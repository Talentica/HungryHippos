/**
 * 
 */
package com.talentica.hungryHippos.manager.zookeeper;

/**
 * @author PooshanS
 *
 */
public class ZookeeperConfiguration {
	


    String hosts;
    String basePath;
    String alertsPath;
    Integer sessionTimeout;
    String configPath;
    String nameSpace;

    /**
     * @param hosts
     * @param basePath
     * @param alertsPath
     * @param sessionTimeout
     * @param configPath
     */
    public ZookeeperConfiguration(String hosts, String basePath,  String alertsPath, Integer sessionTimeout, String configPath, String nameSpace){
    	this.hosts = hosts;
    	this.basePath = basePath;
    	this.alertsPath = alertsPath;
    	this.sessionTimeout = sessionTimeout;
    	this.configPath = configPath;
    	this.nameSpace = nameSpace;
    }
    
    public String getBasePath() {
        return basePath;
    }

    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }

    public String getConfigPath() {
        return configPath;
    }

    public void setConfigPath(String configPath) {
        this.configPath = configPath;
    }

    public String getHosts() {
        return hosts;
    }

    public void setHosts(String hosts) {
        this.hosts = hosts;
    }

    public Integer getSessionTimeout() {
        return sessionTimeout;
    }

    public void setSessionTimeout(Integer sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    public String getAlertsPath() {
        return alertsPath;
    }

    public void setAlertsPath(String alertsPath) {
        this.alertsPath = alertsPath;
    }

	public String getNameSpace() {
		return nameSpace;
	}

	public void setNameSpace(String nameSpace) {
		this.nameSpace = nameSpace;
	}


}
