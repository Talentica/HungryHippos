/**
 * 
 */
package com.talentica.hungryHippos.utility;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.utility.marshaling.DataLocator;
import com.talentica.hungryHippos.utility.marshaling.FieldTypeArrayDataDescription;

/**
 * @author PooshanS
 *
 */
public class CommonUtil {
	private static final Logger LOGGER = LoggerFactory.getLogger(CommonUtil.class.getName());
	private static Properties prop = Property.getProperties();
	private static String[] datatypes = prop.getProperty("column.datatype-size").split(",");
	
	/**
	 * Populate the description fields based on configuration specification
	 *  
	 * @param dataDescription
	 */
	public static void setDataDescription(FieldTypeArrayDataDescription dataDescription){
	        String[] datatype_size;
	        String datatype;
	        int size;
	        if(datatypes == null || datatypes.length == 0){
	        	LOGGER.warn("\n\t No data type is specified in the configuration file");
	        	return;
	        }
	        for(int index=0 ; index<datatypes.length ; index++){
	        	datatype_size = datatypes[index].split("-");
	        	datatype = datatype_size[0];
	        	size = Integer.valueOf(datatype_size[1]);
	        	switch(datatype){
	        		case "STRING":
	        			dataDescription.addFieldType(DataLocator.DataType.STRING,size);
	        			break;
	        		case "DOUBLE":
	        			dataDescription.addFieldType(DataLocator.DataType.DOUBLE,size);
	        			break;
	        		case "FLOAT":
	        			dataDescription.addFieldType(DataLocator.DataType.DOUBLE,size);
	        			break;
	        		case "CHAR":
	        			dataDescription.addFieldType(DataLocator.DataType.CHAR,size);
	        			break;
	        		case "LONG":
	        			dataDescription.addFieldType(DataLocator.DataType.LONG,size);
	        			break;
	        		case "INT":
	        			dataDescription.addFieldType(DataLocator.DataType.INT,size);
	        			break;
	        		case "SHORT":
	        			dataDescription.addFieldType(DataLocator.DataType.SHORT,size);
	        			break;
	        		case "BYTE":
	        			dataDescription.addFieldType(DataLocator.DataType.BYTE,size);
	        			break;
	        		default:
	        			LOGGER.info("\n\t {} datatype is undefined",datatype);
	        	}
	        	
	        }
		
	}
	
	public enum ZKJobNodeEnum{
		
		PUSH_JOB_NOTIFICATION("PUSH_JOB"),PULL_JOB_NOTIFICATION("PULL_JOB"),START("START"),FINISH("FINISH");
		
		private String jobNode;
		
		private ZKJobNodeEnum(String jobNode){
			this.jobNode = jobNode;
		}
		
		public String getZKJobNode(){
			return this.jobNode;
		}
		
	}
}
