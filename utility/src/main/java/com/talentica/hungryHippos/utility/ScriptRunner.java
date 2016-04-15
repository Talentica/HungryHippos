package com.talentica.hungryHippos.utility;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ScriptRunner {

	private static final String SPACE = " ";
	private static final Logger LOGGER = LoggerFactory.getLogger(ScriptRunner.class);

	private static String executeCommand(String scriptCommand, String scriptFilePath) {
		StringBuilder output = new StringBuilder();
		try {
			Runtime rt = Runtime.getRuntime();
			Process process = rt.exec(scriptCommand + SPACE + scriptFilePath);
			// process.waitFor();
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

	/**
	 * Runs a shell script and returns script execution result i.e. whatever was
	 * output to console while script was getting executed.
	 * 
	 * @param scriptCommand
	 * @param scriptFilePath
	 * @return
	 */
	public static String executeShellScript(String scriptFilePath, String... arguments) {
		String args = StringUtils.EMPTY;

		for (int i = 0; i < arguments.length; i++) {
			if (i > 0) {
				args = args + SPACE;
			}
			args = args + arguments[i];
		}
		return executeCommand("/bin/sh", scriptFilePath + SPACE + args);
	}

}
