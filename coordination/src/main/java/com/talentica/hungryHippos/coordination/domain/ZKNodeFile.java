/**
 * 
 */
package com.talentica.hungryHippos.coordination.domain;

import java.io.Serializable;
import java.util.Properties;

/**
 * This is generic file to save on the ZK node and available in distributed system to all other nodes.
 * 
 * @author PooshanS
 *
 */
public class ZKNodeFile implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -6751161826131428942L;
	private String fileName;
	private Properties fileData;
	private Object obj;
	
	public ZKNodeFile(String fileName,Properties fileData,Object ...obj){
		this.fileName = fileName;
		this.fileData = fileData;
		this.obj = (obj!=null && obj.length==1)?obj[0]:null;
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

	public Object getObj() {
		return obj;
	}

	public void setObj(Object obj) {
		this.obj = obj;
	}
	
	
	

}
