package com.talentica.hungryHippos.node;

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.config.cluster.Node;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.List;

/**
 * Created by rajkishoreh on 22/7/16.
 */
public enum NodeInfo {
	INSTANCE;

	private String ip;
	private String id;
    private int identifier;
	private int port;

    NodeInfo() {
        ClusterConfig config = CoordinationConfigUtil.getZkClusterConfigCache();
        List<Node> nodeList = config.getNode();
        Enumeration e;
        boolean configPresent = false;
        try {
            e = NetworkInterface.getNetworkInterfaces();
        } catch (SocketException e1) {
            throw new AssertionError(e1);
        }
        searchIp:
        while (e.hasMoreElements()) {
            NetworkInterface n = (NetworkInterface) e.nextElement();
            Enumeration ee = n.getInetAddresses();
            while (ee.hasMoreElements()) {
                InetAddress inetAddress = (InetAddress) ee.nextElement();
                for (Node node : nodeList) {
                    if (inetAddress.getHostAddress().equals(node.getIp())) {
                        this.ip = node.getIp();
                        this.identifier = node.getIdentifier();
                        this.id = node.getIdentifier()+"";
                        this.port = Integer.parseInt(node.getPort());
                        configPresent = true;
                        break searchIp;
                    }
                }
            }
        }
        if(!configPresent){
            throw new RuntimeException("Unable to find Corresponding Node in Cluster Configuration");
        }
    }

	public String getIp() {
		return ip;
	}

	public String getId() {
		return id;
	}

    public int getIdentifier() {
        return identifier;
    }

    public int getPort() {
		return port;
	}
}
