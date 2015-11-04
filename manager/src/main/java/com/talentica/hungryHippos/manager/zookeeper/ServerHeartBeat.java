/**
 * 
 */
package com.talentica.hungryHippos.manager.zookeeper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.manager.zookeeper.Server.ServerStatus;

/**
 * @author PooshanS
 *
 */
public class ServerHeartBeat {
	private ByteBuffer buffer;
	private AsynchronousSocketChannel client;
	private NodesManager nodesManager;
	private InetSocketAddress hostAddress;
	private String token;
	private static final Logger LOGGER = LoggerFactory.getLogger(ServerHeartBeat.class);
	public void startPinging(Server server){
		try {
			client = AsynchronousSocketChannel.open();
			token = UUID.randomUUID().toString(); // generate token
			byte[] bytes = new String(token).getBytes();
			buffer = ByteBuffer.wrap(bytes);
			hostAddress = new InetSocketAddress(server.getServerAddress()
					.getIp(), server.getPort());
			Future<Void> connected = client.connect(hostAddress);
			connected.get();
			Future<Integer> result = client.write(buffer);
			LOGGER.info("TOKEN SENT TO SERVER IP : {} and NAME : {}",
							new Object[]{server.getServerAddress().getIp(), server.getName()});
			while (!result.isDone()) {
			}
			LOGGER.info("TOKEN :: "+new String(buffer.array()).trim());
			buffer.clear();
			LOGGER.info("TOKEN RECIEVED!!");
			if (server.getServerStatus() == ServerStatus.ACTIVE || server.getServerStatus() == null) {
				nodesManager.checkZookeeperConnection(server);
				server.setServerStatus(ServerStatus.ACTIVE);
				LOGGER.info("SERVER ::  ["+ server.getServerAddress().getIp()	+ "] IS RUNNING, STATUS :: "+server.getServerStatus());
			} else if(server.getServerStatus() == ServerStatus.INACTIVE){
				server.setServerStatus(ServerStatus.ACTIVE);
				nodesManager.createNode(nodesManager
						.buildMonitorPathForServer(server));
			 }
		} catch (AsynchronousCloseException  ex) {
			deleteServerNode(server,ex);
		} catch (IOException e) {
			deleteServerNode(server,e);
		} catch (InterruptedException e) {
			deleteServerNode(server,e);
		} catch (ExecutionException e) {
			deleteServerNode(server,e);
		} finally {
			if (buffer != null) {
				buffer.clear();
			}
			if (client != null) {
				try {
					client.close();
				} catch (IOException e) {
					LOGGER.info("Unable to close client");
				}
			}
		}
	}
	public void deleteServerNode(Server server,Exception ex){
		server.setServerStatus(ServerStatus.INACTIVE);
		LOGGER.warn("\nSERVER is NOT running... IP :: ["
				+ server.getServerAddress().getIp() + "] STATUS :: "
				+ server.getServerStatus());
		nodesManager.deleteNode(server);
	}

	public NodesManager init() throws Exception {
		if (nodesManager == null) {
			nodesManager = new NodesManager();			
		}
		return nodesManager;
	}

	public void deleteAllNodes(String node) throws InterruptedException,
			KeeperException, Exception {
		init().deleteAllNodes(node);
	}
	
	public List<Server> getMonitoredServers() throws InterruptedException{
		return nodesManager.getServers();
	}

}
