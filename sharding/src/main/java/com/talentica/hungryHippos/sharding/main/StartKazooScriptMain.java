/**
 * 
 */
package com.talentica.hungryHippos.sharding.main;


/**
 * @author PooshanS
 *
 */
public class StartKazooScriptMain {/*

	private static final Logger logger = LoggerFactory
			.getLogger(StartKazooScriptMain.class);
	private static String jobUUId;

	public static void main(String[] args) {
		jobUUId = args[0];
		CommonUtil.loadDefaultPath(jobUUId);
		PropertyOld.initialize(PROPERTIES_NAMESPACE.NODE);
		callCopyScriptToRunKazoo();
	}
	
	private static void callCopyScriptToRunKazoo() {
		logger.info("Calling script file to start kazoo server");
		String jobuuid = PropertyOld.getProperties().getProperty("job.uuid");
		String webserverIp = PropertyOld.getProperties().getProperty(
				"common.webserver.ip");
		String[] strArr = new String[] { "/bin/sh", "start-kazoo-server.sh",
				jobuuid, webserverIp };
		CommonUtil.executeScriptCommand(strArr);
		logger.info("Done.");

	}

*/}
