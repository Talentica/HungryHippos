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

	private String ip;
	private String id;
	private int port;

	NodeInfo() {
		ClusterConfig config = CoordinationApplicationContext.getCoordinationConfig().getClusterConfig();
		List<Node> nodeList = config.getNode();
		Enumeration<NetworkInterface> e;
		boolean configPresent = false;
		try {
			e = NetworkInterface.getNetworkInterfaces();
		} catch (SocketException e1) {
			throw new AssertionError(e1);
		}
		searchIp: while (e.hasMoreElements()) {
			NetworkInterface n = (NetworkInterface) e.nextElement();
			Enumeration<InetAddress> ee = n.getInetAddresses();
			while (ee.hasMoreElements()) {
				InetAddress inetAddress = (InetAddress) ee.nextElement();
				for (Node node : nodeList) {
					if (inetAddress.getHostAddress().equals(node.getIp())) {
						this.ip = node.getIp();
						this.id = node.getIdentifier() + "";
						this.port = Integer.parseInt(node.getPort());
						configPresent = true;
						break searchIp;
					} else if (node.getIp().equals("localhost")) {
						this.ip = node.getIp();
						this.id = node.getIdentifier() + "";
						this.port = Integer.parseInt(node.getPort());
						configPresent = true;
					}
				}
			}
		}
		if (!configPresent) {
			throw new RuntimeException("Unable to find Corresponding Node in Cluster Configuration");
		}
	}

	public String getIp() {
		return ip;
	}

	public String getId() {
		return id;
	}

	public int getPort() {
		return port;
	}
}
