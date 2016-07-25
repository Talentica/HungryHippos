package com.talentica.hungryHippos.node;

import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryhippos.config.coordination.ClusterConfig;
import com.talentica.hungryhippos.config.coordination.Node;

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

    private String nodeIp;

    NodeInfo() {
        ClusterConfig config = CoordinationApplicationContext.getCoordinationConfig().getClusterConfig();
        List<Node> nodeList = config.getNode();
        Enumeration e;
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
                        this.nodeIp = node.getIp();
                        break searchIp;
                    }
                }
            }
        }
    }

    public String getNodeIp() {
        return nodeIp;
    }
}
