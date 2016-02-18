/**
 * 
 */
package com.talentica.hungryHippos.utility;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author PooshanS
 *
 */
public class CommonUtil {

	private static final Logger LOGGER = LoggerFactory.getLogger(CommonUtil.class.getName());

	public enum ZKJobNodeEnum{
		
		PUSH_JOB_NOTIFICATION("PUSH_JOB"),PULL_JOB_NOTIFICATION("PULL_JOB"),START_ROW_COUNT("START_ROW_COUNT"),START_JOB_MATRIX("START_JOB_MATRIX"),FINISH_JOB_MATRIX("FINISH_JOB_MATRIX"),FINISH_ROW_COUNT("FINISH_ROW_COUNT");
		
		private String jobNode;
		
		private ZKJobNodeEnum(String jobNode){
			this.jobNode = jobNode;
		}
		
		public String getZKJobNode(){
			return this.jobNode;
		}
		
	}
	
	/**
	 * To Dump the file on the Disk of system.
	 * @param fileName
	 * @param obj
	 */
	public static void dumpFileOnDisk(String fileName, Object obj) {
		String filePath = null;
		try {
			filePath = new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath()
					+ PathUtil.FORWARD_SLASH + fileName;
		} catch (IOException e1) {
			LOGGER.info("Unable to create path for File {}", fileName);
			return;
		}
		LOGGER.info("Dumping File {} AND Path {}", fileName, filePath);
		try (ObjectOutputStream out = new ObjectOutputStream(
				new FileOutputStream(filePath))) {
			out.writeObject(obj);
			out.flush();
		} catch (IOException e) {
			LOGGER.info("There occured some problem to dump file {}", fileName);
		}
	}
	
}
