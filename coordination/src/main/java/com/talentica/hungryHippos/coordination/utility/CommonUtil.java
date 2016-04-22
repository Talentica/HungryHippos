/**
 * 
 */
package com.talentica.hungryHippos.coordination.utility;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.ZKUtils;
import com.talentica.hungryHippos.coordination.domain.ServerHeartBeat;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
import com.talentica.hungryHippos.utility.PathUtil;

/**
 * @author PooshanS
 *
 */
public class CommonUtil {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(CommonUtil.class.getName());

	private static FieldTypeArrayDataDescription dataDescription;

	public static String TEMP_FOLDER_PATH = null;
	
	static{
		if(OSValidator.isWindows()){
			TEMP_FOLDER_PATH = Paths.get("../tmp").toAbsolutePath().toString()+PathUtil.FORWARD_SLASH;
		}else if(OSValidator.isUnix()){
			TEMP_FOLDER_PATH = Paths.get("../tmp").toAbsolutePath().toString()+PathUtil.FORWARD_SLASH;
		}
		LOGGER.info("tmp directory path is {}",TEMP_FOLDER_PATH);
	}
	

	public static final String MASTER_IP_FILE_NAME = "master_ip_file";
	
	public static final String OUTPUT_IP_FILE_NAME = "output_ip_file";
	
	public static final String DROPLET_IDS_FILE_NAME = "droplet_ids_file";
	
	public static final String WEBSERVER_IP_FILE_NAME = "webserver_ip_file";

	public static final String MASTER_IP_FILE_NAME_ABSOLUTE_PATH = TEMP_FOLDER_PATH
			+ MASTER_IP_FILE_NAME;
	
	public static final String OUTPUT_IP_FILE_NAME_ABSOLUTE_PATH = TEMP_FOLDER_PATH + OUTPUT_IP_FILE_NAME;
	
	public static final String DROPLET_IDS_FILE_PATH = TEMP_FOLDER_PATH + DROPLET_IDS_FILE_NAME;
	
	public static final String WEBSERVER_IP_FILE_PATH = TEMP_FOLDER_PATH + WEBSERVER_IP_FILE_NAME;
	
	private static NodesManager nodesManager;
	
	private static UUID UUId;

	public enum ZKJobNodeEnum {

		PUSH_JOB_NOTIFICATION("PUSH_JOB"), PULL_JOB_NOTIFICATION("PULL_JOB"), START_ROW_COUNT(
				"START_ROW_COUNT"), START_JOB_MATRIX("START_JOB_MATRIX"), FINISH_JOB_MATRIX(
				"FINISH_JOB_MATRIX"), FINISH_ROW_COUNT("FINISH_ROW_COUNT"),DOWNLOAD_FINISHED("DOWNLOAD_FINISHED");

		private String jobNode;

		private ZKJobNodeEnum(String jobNode) {
			this.jobNode = jobNode;
		}

		public String getZKJobNode() {
			return this.jobNode;
		}

	}

	/**
	 * To Dump the file on the Disk of system.
	 * 
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

	public static final FieldTypeArrayDataDescription getConfiguredDataDescription() {
		if (dataDescription == null) {
			dataDescription = FieldTypeArrayDataDescription
					.createDataDescription(Property.getDataTypeConfiguration());
		}
		return dataDescription;
	}

	public static List<String> readFile(File fin) throws IOException {
		List<String> listOfLine = new ArrayList<>();
		FileInputStream fis = new FileInputStream(fin);
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
		String line = null;
		while ((line = br.readLine()) != null) {
			listOfLine.add(line);
		}
		br.close();
		return listOfLine;
	}

	public static String getZKIp() {
		try {
			return readFile(new File(MASTER_IP_FILE_NAME_ABSOLUTE_PATH)).get(0);
		} catch (IOException e) {
			LOGGER.info("Unable to read the file zk ip file.");
		}
		return StringUtils.EMPTY;
	}
	
	public static String getKazooIp() throws IOException {
		return readFile(new File(OUTPUT_IP_FILE_NAME_ABSOLUTE_PATH)).get(0);
	}

	public static NodesManager connectZK() throws Exception {
		if(nodesManager == null){
			(nodesManager= ServerHeartBeat.init()).connectZookeeper(getZKIp()).startup();
		}else {
			return nodesManager;
		}
		return nodesManager;
	}

	public static Properties getMergedConfigurationPropertyFromZk()
			throws Exception {
		ZKNodeFile configProp = ZKUtils.getConfigZKNodeFile(Property.MERGED_CONFIG_PROP_FILE);
		if(configProp == null) return null;
		return configProp.getFileData();
	}
	
	public static Properties getServerConfigurationPropertyFromZk()
			throws Exception {
		ZKNodeFile serverConfig = ZKUtils.getConfigZKNodeFile(Property.SERVER_CONF_FILE);
		return (serverConfig == null ) ? null :serverConfig.getFileData();
	}
	
	/**
	 * First args is script name and second one is command
	 * 
	 * @param shellCommand
	 */
	public static void executeScriptCommand(String[] strArr){
		try {
			Runtime rt = Runtime.getRuntime();
			Process pr = rt.exec(strArr,null,null);
			BufferedReader input = new BufferedReader(new InputStreamReader(
					pr.getInputStream()));
			pr.waitFor();
			String line = "";
			while ((line = input.readLine()) != null) {
				LOGGER.info(line);
			}
		} catch (Exception e) {
			LOGGER.info("Execption {}",e);
		}
	}
	
	
/*	public static void generateJobUUID(){
		UUId = UUID.randomUUID();
	}
*/	
	public static String getJobUUIdInBase64(String jobUUId){
		return uuidToBase64(jobUUId);
	}
		
	private static String uuidToBase64(String str) {
	    return Base64.getUrlEncoder().encodeToString(str.getBytes());
	}
	
	/*private static String uuidFromBase64(String str) {
	    return new String(Base64.getUrlDecoder().decode(str.getBytes()));
	    
	}*/

}
