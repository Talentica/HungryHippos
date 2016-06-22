/**
 * 
 */
package com.talentica.hungryHippos.sharding.main;


/**
 * @author PooshanS
 *
 */
public class StartProcessDBEntryMain {/*

	private static final Logger logger = LoggerFactory
			.getLogger(StartProcessDBEntryMain.class);
	private static String jobUUId;

	public static void main(String[] args) {
		validateProgramArguments(args);
		jobUUId = args[0];
		CommonUtil.loadDefaultPath(jobUUId);
		PropertyOld.initialize(PROPERTIES_NAMESPACE.NODE);
		callProcessAfterShardingScript();
	}
	
	private static void callProcessAfterShardingScript() {
		String jobuuid = PropertyOld.getProperties().getProperty("job.uuid");
		String webserverIp = PropertyOld.getProperties().getProperty(
				"common.webserver.ip");
		logger.info(
				"Calling process db python script and uuid {} webserver ip {}",
				jobuuid, webserverIp);
		String pythonScriptPath = Paths
				.get("/root/hungryhippos/scripts/python_scripts")
				.toAbsolutePath().toString()
				+ PathUtil.SEPARATOR_CHAR;
		String[] strArr = new String[] { "/usr/bin/python",
				pythonScriptPath + "processes-db-entries.py", jobuuid,
				webserverIp};
		CommonUtil.executeScriptCommand(strArr);
		logger.info("DB entry is initiated.");
	}

	private static void validateProgramArguments(String[] args) {
		if (args.length < 1) {
			logger.info("please provide  the jobuuid as first argument");
			System.exit(1);
		}
	}


*/}
