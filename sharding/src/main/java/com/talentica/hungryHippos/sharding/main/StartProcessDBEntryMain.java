/**
 * 
 */
package com.talentica.hungryHippos.sharding.main;

import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.Property;
import com.talentica.hungryHippos.coordination.utility.Property.PROPERTIES_NAMESPACE;
import com.talentica.hungryHippos.utility.PathUtil;

/**
 * @author PooshanS
 *
 */
public class StartProcessDBEntryMain {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(StartProcessDBEntryMain.class);
	private static String jobUUId;

	public static void main(String[] args) {
		validateProgramArguments(args);
		jobUUId = args[0];
		CommonUtil.loadDefaultPath(jobUUId);
		Property.initialize(PROPERTIES_NAMESPACE.NODE);
		callProcessAfterShardingScript();
	}
	
	private static void callProcessAfterShardingScript() {
		String jobuuid = Property.getProperties().getProperty("job.uuid");
		String webserverIp = Property.getProperties().getProperty(
				"common.webserver.ip");
		LOGGER.info(
				"Calling process db python script and uuid {} webserver ip {}",
				jobuuid, webserverIp);
		String pythonScriptPath = Paths
				.get("/root/hungryhippos/scripts/python_scripts")
				.toAbsolutePath().toString()
				+ PathUtil.FORWARD_SLASH;
		String[] strArr = new String[] { "/usr/bin/python",
				pythonScriptPath + "processes-db-entries.py", jobuuid,
				webserverIp , "&"};
		CommonUtil.executeScriptCommand(strArr);
		LOGGER.info("DB entry is initiated.");
	}

	private static void validateProgramArguments(String[] args) {
		if (args.length < 1) {
			LOGGER.info("please provide  the jobuuid as first argument");
			System.exit(1);
		}
	}


}
