/**
 * 
 */
package com.talentica.hungryHippos.utility;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;

/**
 * @author PooshanS
 *
 */
public class CommonUtil {

	private static final Logger LOGGER = LoggerFactory.getLogger(CommonUtil.class.getName());

	private static FieldTypeArrayDataDescription dataDescription;

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
		LOGGER.info("Dumping of file {} on disk started.", fileName);
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
		LOGGER.info("Dumping of file {} on disk finished.", fileName);
	}
	
	public static void writeLine(String fileName, List<String> lines) throws IOException{
		File fout = new File(fileName);
		FileOutputStream fos = new FileOutputStream(fout);
	 
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
		int totalLine = lines.size();
		for(String line : lines){
			totalLine--;
			bw.write(line);
			if(totalLine!=0)bw.newLine();
		}
		bw.close();
	}
	
	public static final FieldTypeArrayDataDescription getConfiguredDataDescription() {
		if (dataDescription == null) {
			dataDescription = FieldTypeArrayDataDescription.createDataDescription(Property.getDataTypeConfiguration());
		}
		return dataDescription;
	}

}
