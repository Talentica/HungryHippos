/**
 * 
 */
package com.talentica.hungryHippos.utility;

/**
 * @author PooshanS
 *
 */
public enum PropertyEnum {
	LOG4J("log4j"),CONFIG("config"),SERVER_CONFIG("serverConfigFile");
	
	private String fileName;
	
	private PropertyEnum(String fileName){
		this.fileName = fileName;
	}
	
	public String getPropertyFileName(){
		return new StringBuilder(fileName).append(".properties").toString();
	}

}
