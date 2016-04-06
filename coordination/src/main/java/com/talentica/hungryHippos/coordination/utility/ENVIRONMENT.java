/**
 * 
 */
package com.talentica.hungryHippos.coordination.utility;

/**
 * @author PooshanS
 *
 */
public enum ENVIRONMENT {
	
	PROD("prod"),
	TEST("test"),
	LOCAL("local");

	private String configurationPropertiesPrefix;
	
	private static ENVIRONMENT currentEnvironment=LOCAL;
	
	private ENVIRONMENT(String configurationFolderName){
		this.configurationPropertiesPrefix=configurationFolderName;
	}

	public static ENVIRONMENT getCurrentEnvironment() {
		return currentEnvironment;
	}

	public static void setCurrentEnvironment(String environment) {
		currentEnvironment = ENVIRONMENT.valueOf(environment);
	}

	public String getConfigurationPropertiesPrefix() {
		return configurationPropertiesPrefix;
	}

}
