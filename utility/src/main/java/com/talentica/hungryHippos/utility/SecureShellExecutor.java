package com.talentica.hungryHippos.utility;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public final class SecureShellExecutor {

	private static final Logger LOGGER = LoggerFactory.getLogger(SecureShellExecutor.class);

	private String host;

	private String username;

	private String privateKeyFilePath;

	private String password;

	private int port;

	private static final String EXECUTABLE_CHANNEL = "exec";

	public SecureShellExecutor(String host, String username, String privateKeyFilePath, int port) {
		this.host = host;
		this.username = username;
		this.privateKeyFilePath = privateKeyFilePath;
		this.port = port;
	}

	public SecureShellExecutor(String host, String username, String privateKeyFilePath) {
		this(host, username, privateKeyFilePath, 22);
	}

	public SecureShellExecutor(String host, String username, String privateKeyFilePath, String password) {
		this(host, username, privateKeyFilePath, 22);
		this.password = password;

	}

	public SecureShellExecutor(String host, String username) {
		this(host, username, null, 22);
	}

	private Session getSession() throws JSchException {
		JSch securedShell = new JSch();
		if (StringUtils.isNotBlank(privateKeyFilePath)) {
			securedShell.addIdentity(privateKeyFilePath);
		}
		Session session = securedShell.getSession(username, host, port);
		session.setConfig("StrictHostKeyChecking", "no");
		if (StringUtils.isNotBlank(password)) {
			session.setPassword(password);
		}
		return session;
	}

	public List<String> execute(String command) {
		ChannelExec channelExec = null;
		Session session = null;
		try {
			session = getSession();
			session.connect();
			channelExec = (ChannelExec) session.openChannel(EXECUTABLE_CHANNEL);
			InputStream in = channelExec.getInputStream();
			channelExec.setCommand(command);
			channelExec.connect();
			return readCommandExecutionOutput(command, channelExec, in);
		} catch (JSchException | IOException exception) {
			LOGGER.error("Error occurred while executing shell command.", exception);
			throw new RuntimeException(exception);
		} finally {
			closeChannel(channelExec);
			closeSession(session);
		}
	}

	private List<String> readCommandExecutionOutput(String command, ChannelExec channelExec, InputStream in)
			throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		String line;
		List<String> output = new ArrayList<>();
		while ((line = reader.readLine()) != null) {
			output.add(line);
		}
		int exitStatus = channelExec.getExitStatus();
		if (exitStatus != 0) {
			LOGGER.warn("Command:{} executed but with exist status of {}", new Object[] { command, exitStatus });
		}
		return output;
	}

	private void closeSession(Session session) {
		if (session != null) {
			session.disconnect();
		}
	}

	private void closeChannel(ChannelExec channelExec) {
		if (channelExec != null) {
			channelExec.disconnect();
		}
	}

}
