/**
 * 
 */
package com.talentica.hungryHippos.coordination.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author PooshanS
 *
 */
public class ServerAddress {

	private static final Logger LOGGER = LoggerFactory.getLogger(ServerAddress.class);
    String ip;
    String hostname;

    
    ServerAddress() {
    }

    public ServerAddress(String hostname, String ip) {
        this.hostname = hostname;
        this.ip = ip;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ServerAddress that = (ServerAddress) o;

        if (hostname != null ? !hostname.equals(that.hostname) : that.hostname != null)
            return false;
        if (!ip.equals(that.ip)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = ip.hashCode();
        result = 31 * result + (hostname != null ? hostname.hashCode() : 0);
        return result;
    }

}
