package com.talentica.hungryHippos.utility;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ScriptRunner {

	private static final Logger LOGGER = LoggerFactory.getLogger(ScriptRunner.class);

	private static String executeCommand(String scriptFilePath, java.util.Map<String, String> parameters)
			throws InterruptedException {
		StringBuilder output = new StringBuilder();
		try {
			ProcessBuilder processbuilder = new ProcessBuilder(scriptFilePath);
			for (String key : parameters.keySet()) {
				processbuilder.environment().put(key, parameters.get(key));
			}
			Process process = processbuilder.start();
			process.waitFor();
			BufferedReader scriptOutputReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
			String line = "";
			while ((line = scriptOutputReader.readLine()) != null) {
				output.append(line);
			}
		} catch (IOException exception) {
			LOGGER.error("Exception occurred while executing script {}", scriptFilePath, exception);
			throw new RuntimeException(exception);
		}
		return output.toString();
	}

	public static String executeShellScript(String scriptFilePath, Map<String, String> parameters)
			throws InterruptedException {
		return executeCommand(scriptFilePath, parameters);
	}

}
