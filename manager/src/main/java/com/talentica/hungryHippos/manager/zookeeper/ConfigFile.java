/**
 * 
 */
package com.talentica.hungryHippos.manager.zookeeper;

import java.io.Serializable;
import java.util.Properties;

/**
 * @author PooshanS
 *
 */
public class ConfigFile implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -6751161826131428942L;
	private String fileName;
	private Properties fileData;
	
	public ConfigFile(String fileName,Properties fileData){
		this.fileName = fileName;
		this.fileData = fileData;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public Properties getFileData() {
		return fileData;
	}

	public void setFileData(Properties fileData) {
		this.fileData = fileData;
	}
	
	
	

}
