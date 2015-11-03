package com.talentica.hungryHippos.server;

/**
 * 
 */
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.manager.zookeeper.Property;
/**
 * @author PooshanS
 *
 */
public class ServerPing {
	private static final Logger LOGGER = LoggerFactory.getLogger(ServerPing.class
			.getName());

	public static void main(String[] args) throws IOException,
			InterruptedException, ExecutionException {
		if (args.length == 3) {
			String IP = args[0];
			Integer PORT = Integer.valueOf(args[1]);
			Property.CONFIG_PATH = args[2];
			new Property().getProperties();
			while (true) {
				go(IP, PORT);
			}
		} else {
			LOGGER.info("Please provide argument IP AND PORT");
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
}
