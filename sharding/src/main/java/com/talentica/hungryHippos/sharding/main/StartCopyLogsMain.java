/**
 * 
 */
package com.talentica.hungryHippos.sharding.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author PooshanS
 *
 */
public class StartCopyLogsMain {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(StartCopyLogsMain.class);
	private static String jobUUId;

	public static void main(String[] args) {/*
		jobUUId = args[0];
		CommonUtil.loadDefaultPath(jobUUId);
		PropertyOld.initialize(PROPERTIES_NAMESPACE.NODE);
		callCopyLogsScript();
	*/}

	private static void callCopyLogsScript() {/*
		LOGGER.info("Calling script file to start the logs file");
		String jobuuid = PropertyOld.getProperties().getProperty("job.uuid");
		String sqlServerIp = PropertyOld.getProperties().getProperty("common.webserver.ip");
		String pythonScriptPath = Paths.get("/root/hungryhippos/scripts/python_scripts").toAbsolutePath().toString()
				+ PathUtil.SEPARATOR_CHAR;
		String[] strArr = new String[] { "/usr/bin/python", pythonScriptPath + "copy-all-logs-to-nginx.py", jobuuid,
				sqlServerIp };
		CommonUtil.executeScriptCommand(strArr);
		LOGGER.info("Done.");
	*/}
}
