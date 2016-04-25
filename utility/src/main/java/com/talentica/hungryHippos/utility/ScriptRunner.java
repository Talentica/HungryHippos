package com.talentica.hungryHippos.utility;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
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

	/**
	 * First args is script name and second one is command
	 * 
	 * @param shellCommand
	 */
	public static void executeScriptCommand(String[] strArr) {
		try {
			Runtime rt = Runtime.getRuntime();
			Process pr = rt.exec(new String[] { strArr[0], strArr[1] });
			BufferedReader input = new BufferedReader(new InputStreamReader(pr.getInputStream()));
			String line = "";
			while ((line = input.readLine()) != null) {
				LOGGER.info(line);
			}
		} catch (Exception e) {
			LOGGER.info("Execption {}", e);
		}
	}

	private static final String SPACE = " ";

	private static String executeCommand(String scriptCommand, String scriptFilePath) throws InterruptedException {
		StringBuilder output = new StringBuilder();
		try {
			Runtime rt = Runtime.getRuntime();
			Process process = rt.exec(scriptCommand + SPACE + scriptFilePath);
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

	/**
	 * Runs a shell script and returns script execution result i.e. whatever was
	 * output to console while script was getting executed.
	 * 
	 * @param scriptCommand
	 * @param scriptFilePath
	 * @return
	 * @throws InterruptedException
	 */
	public static String executePythonScript(String scriptFilePath, String... arguments) throws InterruptedException {
		String args = StringUtils.EMPTY;
		for (int i = 0; i < arguments.length; i++) {
			if (i > 0) {
				args = args + SPACE;
			}
			args = args + arguments[i];
		}
		return executeCommand("/usr/bin/python", scriptFilePath + SPACE + args);
	}

	public static String executeShellScript(String scriptFilePath, String... arguments) throws InterruptedException {
		String args = StringUtils.EMPTY;
		for (int i = 0; i < arguments.length; i++) {
			if (i > 0) {
				args = args + SPACE;
			}
			args = args + arguments[i];
		}
		return executeCommand("/bin/sh", scriptFilePath + SPACE + args);
	}
	
	public static String executeJava(String jarFilePath, String... arguments) throws InterruptedException {
		String args = StringUtils.EMPTY;
		for (int i = 0; i < arguments.length; i++) {
			if (i > 0) {
				args = args + SPACE;
			}
			args = args + arguments[i];
		}
		return executeCommand("/usr/bin/java", jarFilePath + SPACE + args);
	}

}
