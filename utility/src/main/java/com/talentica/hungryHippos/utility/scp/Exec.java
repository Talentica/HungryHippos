/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
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
package com.talentica.hungryHippos.utility.scp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class Exec {
	public static String exec(SecureContext pContext, String pCommand)
			throws JSchException, IOException {
		Session session = pContext.createSession();
		session.connect();
		String result = "";
		Channel channel = session.openChannel("exec");
		((ChannelExec) channel).setCommand(pCommand);
		final ByteArrayOutputStream myOut = new ByteArrayOutputStream();
		((ChannelExec) channel).setErrStream(new PrintStream(myOut));

		InputStream in = null;
		try {
			in = channel.getInputStream();
			channel.connect();
			result = readResult(result, channel, in);
			channel.disconnect();
			session.disconnect();
		} finally {
			IOUtils.closeQuietly(in);
			IOUtils.closeQuietly(myOut);
		}
		if (StringUtils.isNotBlank(myOut.toString())) {
			throw new IllegalStateException(myOut.toString());
		}
		return result;
	}

	private static String readResult(String result, Channel channel,
			InputStream in) throws IOException {
		byte[] tmp = new byte[1024];
		while (true) {
			while (in.available() > 0) {
				int i = in.read(tmp, 0, 1024);
				if (i < 0)
					break;
				result += new String(tmp, 0, i);
			}
			if (channel.isClosed()) {
				break;
			}
		}
		return result;
	}

}