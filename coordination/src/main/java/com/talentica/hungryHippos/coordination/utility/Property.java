/**
 * 
 */
package com.talentica.hungryHippos.coordination.utility;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.utility.ENVIRONMENT;

/**
 * @author PooshanS
 *
 */
public class Property {

	private static final Logger LOGGER = LoggerFactory.getLogger(Property.class
			.getName());
	private static Properties properties = null;
	private static Properties zkProperties = null;
	private static Properties mergeProperties = null;
	private static Properties serverProp = null;
	private static InputStream CONFIG_FILE_INPUT_STREAM;
	private static InputStream ZK_CONFIG_FILE_INPUT_STREAM;
	private static ClassLoader loader = Property.class.getClassLoader();
	private static PROPERTIES_NAMESPACE namespace;
	private static String environmentPropertiesPrefix;
	private static Properties localEnvironmentServerproperties;
	public static final String SERVER_CONFIGURATION_KEY_PREFIX = "server.";

	public static final String CONF_PROP_FILE = "config.properties";
	public static final String SERVER_CONF_FILE = "serverConfigFile.properties";
	public static final String ZK_PROP_FILE = "zookeeper.properties";
	public static final String MERGED_CONFIG_PROP_FILE = "mergedConfig.properties";
	public static boolean isReadFirstTime = true;

	public enum PROPERTIES_NAMESPACE {

		MASTER("master"), NODE("node"), COMMON("common"), ZK("zk");

		private String namespace;

		private PROPERTIES_NAMESPACE(String namespace) {
			this.namespace = namespace;
		}

		public String getNamespace() {
			return namespace;
		}
	}

	public static void overrideConfigurationProperties(
			String configPropertyFilePath) throws FileNotFoundException {
		CONFIG_FILE_INPUT_STREAM = new FileInputStream(configPropertyFilePath);
	}

	public static Properties getProperties() {
		if (mergeProperties == null) {
			mergeProperties = new Properties();
			try {
				if (CONFIG_FILE_INPUT_STREAM != null) {
					if (!isReadFirstTime) {
						try {
							properties = CommonUtil
									.getMergedConfigurationPropertyFromZk();
							if (properties != null)
								return properties;
						} catch (Exception e1) {
							LOGGER.info("Unable to get the config file from zk.");
						}
					}
					if (properties == null) {
						properties = new Properties();
						LOGGER.info("External configuration properties file is loaded");
						properties.load(CONFIG_FILE_INPUT_STREAM);
						/* Load zookeeper configuration file */
						if(zkProperties == null){
							zkProperties = new Properties();
						ZK_CONFIG_FILE_INPUT_STREAM = loader
								.getResourceAsStream(ZK_PROP_FILE);
						zkProperties.load(ZK_CONFIG_FILE_INPUT_STREAM);
						isReadFirstTime = false;
						}
					}
				} else {
					LOGGER.info("Internal configuration properties file is loaded");
					try {
						properties = CommonUtil
								.getMergedConfigurationPropertyFromZk();
						if (properties != null)
							return properties;
					} catch (Exception e) {
						LOGGER.info("Unable to get the property file from zk node.");
					}
					if (properties == null) {
						properties = new Properties();
						CONFIG_FILE_INPUT_STREAM = loader
								.getResourceAsStream(CONF_PROP_FILE);
						properties.load(CONFIG_FILE_INPUT_STREAM);
						if (zkProperties == null) {
							zkProperties = new Properties();
							/* Load zookeeper configuration file */
							ZK_CONFIG_FILE_INPUT_STREAM = loader
									.getResourceAsStream(ZK_PROP_FILE);
							zkProperties.load(ZK_CONFIG_FILE_INPUT_STREAM);
						}
					}
				}
				// PropertyConfigurator.configure(properties);
			} catch (IOException e) {
				LOGGER.info("Unable to load the property file!!");
			}
			mergeProperties.putAll(properties);
			mergeProperties.putAll(zkProperties);
			PropertyConfigurator.configure(mergeProperties);
		}
		return mergeProperties;
	}

	public static Properties loadServerProperties() {
		if (ENVIRONMENT.getCurrentEnvironment() == ENVIRONMENT.LOCAL) {
			return localEnvironmentServerproperties;
		}
		if (serverProp == null) {
			try {
				serverProp = CommonUtil.getServerConfigurationPropertyFromZk();
			} catch (Exception e1) {
				LOGGER.info("Unable to get the server configuration file from zk node.");
			}
			if (serverProp == null) {
				serverProp = new Properties();
				try {
					InputStream is = new FileInputStream(
							CommonUtil.TEMP_FOLDER_PATH
									+ Property.SERVER_CONF_FILE);
					serverProp.load(is);
					// serverProp.load(loader.getResourceAsStream(SERVER_CONF_FILE));
					PropertyConfigurator.configure(serverProp);
					LOGGER.info("serverConfigFile.properties file is loaded");
				} catch (IOException e) {
					LOGGER.warn("Unable to load serverConfigFile.properties file");
				}
			}
		}
		return serverProp;
	}

	public static int getTotalNumberOfNodes() {
		Properties serverProperties = Property.loadServerProperties();
		if (ENVIRONMENT.getCurrentEnvironment() == ENVIRONMENT.LOCAL) {
			return 1;
		}
		int totalNumberOfNodes = 0;
		for (Object key : serverProperties.keySet()) {
			if (StringUtils.startsWith(key.toString(),
					SERVER_CONFIGURATION_KEY_PREFIX)) {
				totalNumberOfNodes++;
			}
		}
		return totalNumberOfNodes;
	}

	public static String getPropertyValue(String propertyName) {
		Properties properties = getProperties();
		if (namespace != null) {
			Object propertyValue = properties.get(environmentPropertiesPrefix
					+ "." + namespace.getNamespace() + "." + propertyName);
			if (propertyValue != null) {
				return propertyValue.toString();
			}
			propertyValue = properties.get(namespace.getNamespace() + "."
					+ propertyName);
			if (propertyValue != null) {
				return propertyValue.toString();
			}
			propertyValue = properties.get(environmentPropertiesPrefix + "."
					+ PROPERTIES_NAMESPACE.COMMON.getNamespace() + "."
					+ propertyName);
			if (propertyValue != null) {
				return propertyValue.toString();
			}
			propertyValue = properties.get(PROPERTIES_NAMESPACE.COMMON
					.getNamespace() + "." + propertyName);
			if (propertyValue != null) {
				return propertyValue.toString();
			}
		}
		Object propertyValue = properties.get(environmentPropertiesPrefix + "."
				+ propertyName);
		if (propertyValue != null) {
			return propertyValue.toString();
		}
		return properties.get(propertyName).toString();
	}

	public static final void initialize(PROPERTIES_NAMESPACE appNamespace) {
		ENVIRONMENT.setCurrentEnvironment(getPropertyValue("environment")
				.toString());
		if (ENVIRONMENT.getCurrentEnvironment() == ENVIRONMENT.LOCAL) {
			localEnvironmentServerproperties = new Properties();
			localEnvironmentServerproperties.put(
					SERVER_CONFIGURATION_KEY_PREFIX + "0", "localhost:2324");
		}
		environmentPropertiesPrefix = ENVIRONMENT.getCurrentEnvironment()
				.getConfigurationPropertiesPrefix();
		namespace = appNamespace;
	}

	public static String[] getShardingDimensions() {
		String keyOrderString = getPropertyValue("common.sharding_dimensions")
				.toString();
		return keyOrderString.split(",");
	}

	public static String[] getKeyNamesFromIndexes(int[] keyIndexes) {
		String[] keyColumnNames = Property
				.getPropertyValue("common.column.names").toString().split(",");
		String[] result = new String[keyIndexes.length];
		for (int i = 0; i < keyIndexes.length; i++) {
			result[i] = keyColumnNames[keyIndexes[i]];
		}
		return result;
	}

	public static void setOrOverrideConfigurationProperty(String key,
			String value) {
		getProperties().setProperty(key, value);
	}

	public static final String[] getDataTypeConfiguration() {
		return getPropertyValue("column.datatype-size").toString().split(",");
	}

	public static PROPERTIES_NAMESPACE getNamespace() {
		return namespace;
	}

}
