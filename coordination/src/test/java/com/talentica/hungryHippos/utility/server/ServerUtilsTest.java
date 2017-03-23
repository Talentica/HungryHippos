/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.talentica.hungryHippos.utility.server;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.talentica.hungryHippos.coordination.server.ServerUtils;

@Ignore
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
