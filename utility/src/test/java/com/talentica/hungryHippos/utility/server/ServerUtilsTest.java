package com.talentica.hungryHippos.utility.server;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;

import org.junit.Assert;
import org.junit.Test;

import com.talentica.hungryHippos.utility.server.ServerUtils;

public class ServerUtilsTest {
	
	@Test
	public void testConnectToServerToGoogle() throws UnknownHostException, IOException, InterruptedException{
		Socket socket = ServerUtils.connectToServer("google.com:80", 3);
		Assert.assertNotNull(socket);
	}
	
	@Test(expected=ConnectException.class)
	public void testConnectToRandomServerRandomPort() throws UnknownHostException, IOException, InterruptedException{
		Socket socket = ServerUtils.connectToServer("104.236.33.13:2322", 1);
		Assert.assertNotNull(socket);
	}

}
