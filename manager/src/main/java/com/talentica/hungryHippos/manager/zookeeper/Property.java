/**
 * 
 */
package com.talentica.hungryHippos.manager.zookeeper;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author PooshanS
 *
 */
public class Property{	
	
	private Properties prop = null;
	public static String CONFIG_PATH;
	
	public Properties getProperties() {
		if (prop == null) {
			InputStream in;
			try {
				prop = new Properties();
				in = new FileInputStream(CONFIG_PATH);
				prop.load(in);

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return prop;
	}

}
