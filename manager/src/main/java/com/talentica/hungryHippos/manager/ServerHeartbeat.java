/**
 * 
 */
package com.talentica.hungryHippos.manager;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.URI;
import java.net.UnknownHostException;

import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

/**
 * ServerHeartbeat class
 * 
 * @author PooshanS
 *
 */
public class ServerHeartbeat {

	private Socket socket = null;
	
	private String serverIP;

	private int port;
	
	DataOutputStream  oos = null;
	DataInputStream  ois = null;

	public ServerHeartbeat(String serverIP, int port) {
		this.serverIP = serverIP;
		this.port = port;
	}

	public String getServerIP() {
		return serverIP;
	}

	public void setServerIP(String serverIP) {
		this.serverIP = serverIP;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}
	
	/**
	 * Ping to check server is working
	 * 
	 * @param serverIP
	 * @return true/false
	 * @throws IOException 
	 */
	public synchronized boolean isPingWorking(){
		boolean isConnected = false;
		OutputStream os = null;
        OutputStreamWriter osw = null;
        BufferedWriter bw = null;
        InputStream is = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        
		//byte[] reqBytes = new byte[4];
		try {
			socket = new Socket(serverIP, port);
			os = socket.getOutputStream();
			osw = new OutputStreamWriter(os);
			bw = new BufferedWriter(osw);
	        
	        
		//	ByteBuffer req = ByteBuffer.wrap(reqBytes);
		//	req.putInt(ByteBuffer.wrap("test".getBytes()).getInt());
					
			
            String msg = "Test";
			System.out.println("Sending request to Socket Server");
            bw.write(msg);
            bw.flush();
            
            is = socket.getInputStream();
	        isr = new InputStreamReader(is);
	        br = new BufferedReader(isr);
	        while(br.ready()){
            String message = br.readLine();
            System.out.println(message);
	        }
                       
            isConnected = true;
		} catch (Exception e) {
			isConnected = false;
			e.printStackTrace();
		}finally{
			try {
				socket.close();
				os.close();
				osw.close();
	            bw.close();
	            is.close();
				isr.close();
	            br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
		return isConnected;
	}

	@Override
	public String toString() {
		return "ServerHeartbeat [serverIP=" + serverIP + ", port=" + port
				+ ", getServerIP()=" + getServerIP() + ", getPort()="
				+ getPort() + ", isPingWorking()=" + isPingWorking()
				+ ", getClass()=" + getClass() + ", hashCode()=" + hashCode()
				+ ", toString()=" + super.toString() + "]";
	}
	
	/*public void reconnectServer(Socket socket){
		byte[] reqBytes = new byte[4];
		try {	
		socket = new Socket();
		System.out.println("Reconnecting IP :: " + serverIP);
		socket.setSoLinger(false, 10);
		socket.setSoTimeout(20000);			
		socket.connect(new InetSocketAddress(serverIP,port));
		InputStream is = socket.getInputStream();
        OutputStream os = socket.getOutputStream();
        os.write(reqBytes);
        byte[] resBytes = new byte[4];
        int rc = is.read(resBytes);
        String retv = new String(resBytes);
        System.out.println("rc=" + rc + " retv=" + retv);
	}catch (Exception e) {
		System.out.println("IP :: "+ serverIP +" disconnected due to :: " + e.getMessage());
	}
	}*/
	
	public boolean webSocketConn() throws UnknownHostException, IOException{
		boolean isTrue = false;
		WebSocketClient client = new WebSocketClient();
		socket = new Socket(serverIP, port);
		try {
            client.start();
            URI echoUri = new URI("ws://159.203.72.1:22");
            ClientUpgradeRequest request = new ClientUpgradeRequest();
            client.connect(socket, echoUri, request);
            System.out.printf("Connecting to : %s%n", echoUri);
           // socket.awaitClose(5, TimeUnit.SECONDS);
            socket.wait(5000);
            isTrue = client.isStarted();
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            try {
                client.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
		return isTrue;
	}
}
