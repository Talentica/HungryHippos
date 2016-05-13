/**
 * 
 */
package com.talentica.hungryHippos.sharding.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.Property;
import com.talentica.hungryHippos.coordination.utility.Property.PROPERTIES_NAMESPACE;

/**
 * @author PooshanS
 *
 */
public class StartKazooScriptMain {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(StartKazooScriptMain.class);
	private static String jobUUId;

	public static void main(String[] args) {
		jobUUId = args[0];
		CommonUtil.loadDefaultPath(jobUUId);
		Property.initialize(PROPERTIES_NAMESPACE.NODE);
		callCopyScriptToRunKazoo();
	}
	
	private static void callCopyScriptToRunKazoo() {
		LOGGER.info("Calling script file to start kazoo server");
		String jobuuid = Property.getProperties().getProperty("job.uuid");
		String webserverIp = Property.getProperties().getProperty(
				"common.webserver.ip");
		String[] strArr = new String[] { "/bin/sh", "start-kazoo-server.sh",
				jobuuid, webserverIp };
		CommonUtil.executeScriptCommand(strArr);
		LOGGER.info("Done.");

	}

}
