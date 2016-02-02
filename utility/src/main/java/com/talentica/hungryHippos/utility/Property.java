/**
 * 
 */
package com.talentica.hungryHippos.utility;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author PooshanS
 *
 */
public class Property {

	private static final Logger LOGGER = LoggerFactory.getLogger(Property.class.getName());
	private static Properties properties = null;
	private static Properties serverProp = null;
	public static InputStream CONFIG_FILE;
	private static ClassLoader loader = Property.class.getClassLoader();
	private static PROPERTIES_NAMESPACE namespace;
	public static final String LOG_PROP_FILE = PropertyEnum.LOG4J.getPropertyFileName();
	public static final String CONF_PROP_FILE = PropertyEnum.CONFIG.getPropertyFileName();
	public static final String SERVER_CONF_FILE = PropertyEnum.SERVER_CONFIG.getPropertyFileName();
	public static final String SERVER_CONFIGURATION_KEY_PREFIX = "server.";

	public enum PROPERTIES_NAMESPACE {

		MASTER("master"), 
		NODE("node"), 
		COMMON("common");

		private String namespace;

		private PROPERTIES_NAMESPACE(String namespace) {
			this.namespace = namespace;
		}

		public String getNamespace() {
			return namespace;
		}

	}

	public static Properties getProperties() {
		if (properties == null) {
			try {
				properties = new Properties();
				if (CONFIG_FILE != null) {
					LOGGER.info("External configuration properties file is loaded");
					properties.load(CONFIG_FILE);
				} else {
					LOGGER.info("Internal configuration properties file is loaded");
					CONFIG_FILE = loader.getResourceAsStream(CONF_PROP_FILE);
					properties.load(CONFIG_FILE);
				}
				PropertyConfigurator.configure(properties);
				LOGGER.info("Property file is loaded!!");
				loadLoggerSetting();
			} catch (IOException e) {
				LOGGER.info("Unable to load the property file!!");
			}
		}
		return properties;
	}

	public static void loadLoggerSetting() {
		Properties prop = new Properties();
		try {
			prop.load(loader.getResourceAsStream(LOG_PROP_FILE));
			PropertyConfigurator.configure(prop);
			LOGGER.info("log4j.properties file is loaded");
		} catch (IOException e) {
			LOGGER.warn("Unable to load log4j.properties file");
		}
	}

	public static Properties loadServerProperties() {
		if (serverProp == null) {
			serverProp = new Properties();
			try {
				serverProp.load(loader.getResourceAsStream(SERVER_CONF_FILE));
				PropertyConfigurator.configure(serverProp);
				LOGGER.info("serverConfigFile.properties file is loaded");
			} catch (IOException e) {
				LOGGER.warn("Unable to load serverConfigFile.properties file");
			}
		}
		return serverProp;
	}

	public static int getTotalNumberOfNodes() {
		Properties serverProperties = Property.loadServerProperties();
		int totalNumberOfNodes = 0;
		for (Object key : serverProperties.keySet()) {
			if (StringUtils.startsWith(key.toString(), SERVER_CONFIGURATION_KEY_PREFIX)) {
				totalNumberOfNodes++;
			}
		}
		return totalNumberOfNodes;
	}

	public static Object getPropertyValue(String propertyName) {
		Properties properties = getProperties();
		if (namespace != null) {
			Object propertyValue = properties.get(namespace.getNamespace() + "." + propertyName);
			if (propertyValue != null) {
				return propertyValue;
			}
			propertyValue = properties.get(PROPERTIES_NAMESPACE.COMMON.getNamespace() + "." + propertyName);
			if (propertyValue != null) {
				return propertyValue;
			}
		}
		return properties.get(propertyName);
	}

	public static final void setNamespace(PROPERTIES_NAMESPACE appNamespace) {
		namespace = appNamespace;
	}

	public static String[] getKeyOrder(){
		String keyOrderString = getPropertyValue("common.keyorder").toString();
		return keyOrderString.split(",");
	}
	
	public static String[] getKeyNamesFromIndexes(int[] keyIndexes) {
		String[] keyOrder = Property.getKeyOrder();
		String[] keyNames = new String[keyIndexes.length];
		for (int i = 0; i < keyIndexes.length; i++) {
			keyNames[i] = keyOrder[keyIndexes[i]];
		}
		return keyNames;
	}
	public static void setOrOverrideConfigurationProperty(String key, String value) {
		getProperties().setProperty(key, value);
	}

}
