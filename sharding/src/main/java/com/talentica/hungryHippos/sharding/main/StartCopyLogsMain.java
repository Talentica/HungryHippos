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
public class StartCopyLogsMain {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(StartCopyLogsMain.class);
	private static String jobUUId;

	public static void main(String[] args) {
		jobUUId = args[0];
		CommonUtil.loadDefaultPath(jobUUId);
		Property.initialize(PROPERTIES_NAMESPACE.NODE);
		callCopyLogsScript();
	}

	private static void callCopyLogsScript() {
		LOGGER.info("Calling script file to start the logs file");
		String jobuuid = Property.getProperties().getProperty("job.uuid");
		String pythonScriptPath = Paths
				.get("/root/hungryhippos/scripts/python_scripts")
				.toAbsolutePath().toString()
				+ PathUtil.FORWARD_SLASH;
		String[] strArr = new String[] { "/usr/bin/python",
				pythonScriptPath + "copy-all-logs-to-nginx.py", jobuuid};
		CommonUtil.executeScriptCommand(strArr);
		LOGGER.info("Done.");
	}
}
