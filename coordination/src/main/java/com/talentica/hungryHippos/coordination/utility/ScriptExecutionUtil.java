/**
 * 
 */
package com.talentica.hungryHippos.coordination.utility;

import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.utility.PathUtil;

/**
 * @author PooshanS
 *
 */
public class ScriptExecutionUtil {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ScriptExecutionUtil.class);

	/**
	 * 
	 */
	public static void callDownloadShellScript() {
		String webserverIp = Property.getProperties().getProperty(
				"common.webserver.ip");
		String jobuuid = Property.getProperties().getProperty("job.uuid");
		String downloadUrlLink = Property.getProperties().getProperty(
				"input.file.url.link");
		LOGGER.info(
				"Calling shell script to download the input file from url link {} for jobuuid {} and webserver ip {}",
				downloadUrlLink, jobuuid, webserverIp);
		String downloadScriptPath = Paths
				.get("/root/hungryhippos/scripts/bash_scripts")
				.toAbsolutePath().toString()
				+ PathUtil.SEPARATOR_CHAR;
		String[] strArr = new String[] { "/bin/sh",
				downloadScriptPath + "download-file-from-url.sh",
				downloadUrlLink, jobuuid, webserverIp };
		CommonUtil.executeScriptCommand(strArr);
		LOGGER.info("Downloading is initiated.");
	}

	/**
	 * 
	 */
	public static void callSamplingShellScript() {
		String jobuuid = Property.getProperties().getProperty("job.uuid");
		String webserverIp = Property.getProperties().getProperty(
				"common.webserver.ip");
		LOGGER.info(
				"Calling sampling python script and uuid {} webserver ip {}",
				jobuuid, webserverIp);
		String samplingScriptPath = Paths
				.get("/root/hungryhippos/scripts/python_scripts")
				.toAbsolutePath().toString()
				+ PathUtil.SEPARATOR_CHAR;
		String[] strArr = new String[] { "/usr/bin/python",
				samplingScriptPath + "sampling-input-file.py", jobuuid,
				webserverIp };
		CommonUtil.executeScriptCommand(strArr);
		LOGGER.info("Sampling is initiated.");
	}

	/**
	 * 
	 */
	public static void callCopyScriptForMapFiles() {
		LOGGER.info("Calling script file to copy map file across all nodes");
		String jobuuid = Property.getProperties().getProperty("job.uuid");
		String[] strArr = new String[] { "/bin/sh",
				"copy-shard-files-to-all-nodes.sh", jobuuid };
		CommonUtil.executeScriptCommand(strArr);
		LOGGER.info("Done.");
	}
	
	/**
	 * @param jobuuid
	 */
	public static void callCopySuccessShellScript(String jobuuid) {
		String downloadScriptPath = Paths.get("../bin")
				.toAbsolutePath().toString()
				+ PathUtil.SEPARATOR_CHAR;
		String[] strArr = new String[] { "/bin/sh",
				downloadScriptPath + "copy-logs-success.sh", jobuuid };
		CommonUtil.executeScriptCommand(strArr);
		LOGGER.info("Copying success logs are initiated");
	}
	
	/**
	 * @param jobuuid
	 */
	public static void callCopyFailureShellScript(String jobuuid) {
		String downloadScriptPath = Paths.get("../bin")
				.toAbsolutePath().toString()
				+ PathUtil.SEPARATOR_CHAR;
		String[] strArr = new String[] { "/bin/sh",
				downloadScriptPath + "copy-log-failure.sh", jobuuid };
		CommonUtil.executeScriptCommand(strArr);
		LOGGER.info("Copying failure logs are initiated");
	}

}
