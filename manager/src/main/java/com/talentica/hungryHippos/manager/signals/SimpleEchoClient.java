/**
 * 
 */
package com.talentica.hungryHippos.manager.signals;

import java.net.URI;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

/**
 * @author PooshanS
 *
 */
public class SimpleEchoClient {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		

        String destUri = "ws://159.203.90.228:22";
        if (args.length > 0) {
            destUri = args[0];
        }
        WebSocketClient client = new WebSocketClient();
        SimpleEchoSocket socket = new SimpleEchoSocket();
        try {
            client.start();
            URI echoUri = new URI(destUri);
            ClientUpgradeRequest request = new ClientUpgradeRequest();
           Future<Session> future = client.connect(socket, echoUri, request);
           System.out.println(future.get().getUpgradeResponse().isSuccess());
            System.out.printf("Connecting to : %s%n", echoUri);
            //socket.onConnect(new WebSocketSession());
            socket.awaitClose(15, TimeUnit.SECONDS);
            
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            try {
                client.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

	}

}
