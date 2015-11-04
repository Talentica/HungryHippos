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
	private Properties prop = null;
	public static InputStream CONFIG_PATH;
	private static final Logger LOGGER = LoggerFactory.getLogger(Property.class.getName());
	public Properties getProperties() {
		if (prop == null) {
			InputStream in = null;
			try {
				prop = new Properties();
				prop.load(CONFIG_PATH);
				PropertyConfigurator.configure(prop);
				LOGGER.info("\n\t Property file is loaded!!");
			} catch (IOException e) {
				LOGGER.info("Unable to load the property file!!");
			}
		}
		return prop;
	}

}
