/**
 * 
 */
package com.talentica.hungryHippos.manager.zookeeper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author PooshanS
 *
 */
public class Property{	
	private static final Logger LOGGER = LoggerFactory.getLogger(Property.class.getName());
	private static Properties prop = null;
	public  static InputStream CONFIG_FILE;
	private static ClassLoader loader = Property.class.getClassLoader();
	public static final String LOG_PROP_FILE = "log4j.properties";
	public static final String CONF_PROP_FILE = "config.properties";
	public static Properties getProperties() {
		if (prop == null) {
			try {
				prop = new Properties();
				if(CONFIG_FILE != null){
				 prop.load(CONFIG_FILE) ;
				 }else{
					 CONFIG_FILE = loader.getResourceAsStream(CONF_PROP_FILE);
					prop.load(CONFIG_FILE) ;
				}
				PropertyConfigurator.configure(prop);
				LOGGER.info("\n\t Property file is loaded!!");
				loadLoggerSetting(); //load logger file
			} catch (IOException e) {
				LOGGER.info("Unable to load the property file!!");
			}
		}
		return prop;
	}
	
	public static void loadLoggerSetting() {
		Properties prop = new Properties();
		try {
			prop.load(loader.getResourceAsStream(LOG_PROP_FILE));
			PropertyConfigurator.configure(prop);
			LOGGER.info("\n\tlog4j.properties file is loaded");
		} catch (IOException e) {
			LOGGER.warn("\n\tUnable to load log4j.properties file");
		}
	}

}
