package com.talentica.hungryHippos.coordination.server;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author PooshanS
 *
 */
public class ServerUtils {

	public static final String PRIFIX_SERVER_NAME = "server";
	public static final String DOT = ".";
	public static final String COLON = ":";
	private static final Logger LOGGER = LoggerFactory.getLogger(ServerUtils.class
			.getName());

	public static Socket connectToServer(String server, int numberOfAttempts)
			throws UnknownHostException, IOException, InterruptedException {
		int tryCount = 0;
		while (true) {
			try {
				tryCount++;
				Socket socket = new Socket(server.split(":")[0].trim(), Integer.valueOf(server.split(":")[1].trim()));
				return socket;
			} catch (ConnectException cex) {
				if (tryCount >= numberOfAttempts) {
					throw cex;
				}
				LOGGER.warn("Connection could not get established. Please start the node {}",
						server.split(":")[0].trim());
				Thread.sleep(5000);
			}
		}
	}
}
