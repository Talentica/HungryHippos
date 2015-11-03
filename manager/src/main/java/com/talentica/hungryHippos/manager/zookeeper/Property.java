/**
 * 
 */
package com.talentica.hungryHippos.manager.zookeeper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.manager.util.PathUtil;

/**
 * @author PooshanS
 *
 */
public class Property{	
	private Properties prop = null;
	public static String CONFIG_PATH = "resources/config.properties";
	private static final Logger LOGGER = LoggerFactory.getLogger(Property.class.getName());
	public Properties getProperties() {
		if (prop == null) {
			InputStream in = null;
			try {
				String currentDirectory = new File(".").getCanonicalPath();
				prop = new Properties();
				in = new FileInputStream(currentDirectory+PathUtil.SLASH+CONFIG_PATH);
				prop.load(in);
				PropertyConfigurator.configure(prop);
				LOGGER.info("\n\t Property file is loaded!!");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return prop;
	}

}
