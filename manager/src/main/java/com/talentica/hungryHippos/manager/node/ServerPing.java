package com.talentica.hungryHippos.manager.node;

/**
 * 
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
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
public class ServerPing {
	private static final String LOG_PROP_FILE = "log4j.properties";
	private static ClassLoader loader = ServerPing.class.getClassLoader();
	static{
		   Property.CONFIG_FILE = loader.getResourceAsStream(LOG_PROP_FILE);
	   }
	private static final Logger LOGGER = LoggerFactory.getLogger(ServerPing.class
			.getName());
	private static final String LINUX_IP_COMMAND = "ip addr | grep 'state UP' -A2 | tail -n1 | awk '{print $2}' | cut -f1  -d'/'";
   	
	public static void main(String[] args) throws IOException,
			InterruptedException, ExecutionException{
		
		if (args.length != 1) {
			LOGGER.info("Please provide argument PORT and config.properties");
		} else {
			String IP = new ServerPing().getLocalIP(LINUX_IP_COMMAND);
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
	
	private String getLocalIP(String command) {
		System.out.println(command.toString());
		StringBuffer output = new StringBuffer();

		Process p;
		try {
			p = Runtime.getRuntime().exec(new String[]{"sh", "-c", command});
			p.waitFor();
			BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
                        String line = "";			
			while ((line = reader.readLine())!= null) {
				output.append(line + "\n");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return output.toString();

	}
}
