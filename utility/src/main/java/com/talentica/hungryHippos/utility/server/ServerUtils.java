package com.talentica.hungryHippos.utility.server;

/**
 * 
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.utility.Property;

/**
 * @author PooshanS
 *
 */
public class ServerUtils {

	public static final String PRIFIX_SERVER_NAME = "server";
	public static final String DOT = ".";
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ServerUtils.class
			.getName());
	private static final String LINUX_IP_COMMAND = "ip addr | grep 'state UP' -A2 | tail -n1 | awk '{print $2}' | cut -f1  -d'/'";
   	
	public static void main(String[] args) throws IOException,
			InterruptedException, ExecutionException{
		
		if (args.length != 1) {
			LOGGER.info("Please provide argument PORT and config.properties");
		} else {
			String IP = new ServerUtils().getLocalIP(LINUX_IP_COMMAND);
			LOGGER.info("IP :: " + IP);
			Integer PORT = Integer.valueOf(args[0]);
			Property.getProperties();
			while (true) {
				go(IP, PORT);
			}
		}
	}

	@SuppressWarnings("rawtypes")
	private static void go(String IP, int PORT) throws IOException,
			InterruptedException, ExecutionException {
		AsynchronousServerSocketChannel serverChannel = AsynchronousServerSocketChannel
				.open();
		InetSocketAddress hostAddress = new InetSocketAddress(IP,
				Integer.valueOf(PORT));
		serverChannel.bind(hostAddress);

		LOGGER.info("Server channel bound to port: " + hostAddress.getPort());
		LOGGER.info("Waiting for client to connect... ");

		Future acceptResult = serverChannel.accept();
		AsynchronousSocketChannel clientChannel = (AsynchronousSocketChannel) acceptResult
				.get();

		LOGGER.info("Messages from client: ");

		if ((clientChannel != null) && (clientChannel.isOpen())) {
				ByteBuffer buffer = ByteBuffer.allocate(32);
				Future result = clientChannel.read(buffer);
				while (!result.isDone()) {
				}
				buffer.flip();
				String message = new String(buffer.array()).trim();
				LOGGER.info(message);
				buffer.clear();
				buffer = ByteBuffer.wrap((message).getBytes());
				Future<Integer> ret = clientChannel.write(buffer);
				while (!ret.isDone()) {
				}
				buffer.flip();
			clientChannel.close();
		}
		serverChannel.close();
	}
	
	private String getLocalIP(String command) throws IOException, InterruptedException {
		LOGGER.info("Executing command: {}", command.toString());
		StringBuffer output = new StringBuffer();
		Process p = Runtime.getRuntime().exec(new String[] { "sh", "-c", command });
		p.waitFor();
		BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String line = "";
		while ((line = reader.readLine()) != null) {
			output.append(line + "\n");
		}
		return output.toString();
	}

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
