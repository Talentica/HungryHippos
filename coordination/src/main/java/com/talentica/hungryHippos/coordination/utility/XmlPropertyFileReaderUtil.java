package com.talentica.hungryHippos.coordination.utility;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.InvalidPropertiesFormatException;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XmlPropertyFileReaderUtil {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(XmlPropertyFileReaderUtil.class.getName());

	private static Map<String, Properties> cacheResources;

	private static Class<?> getClass(String classNameWithPackage) {
		Class<?> clazz = null;
		try {
			clazz = ClassUtils.getClass(classNameWithPackage);
		} catch (ClassNotFoundException e) {
			LOGGER.info("Unable to fina the class");
		}
		return clazz;
	}

	private static ClassLoader getClassLoader(String classNameWithPackage) {
		ClassLoader loader;
		Class<?> clazz = getClass(classNameWithPackage);
		loader = clazz.getClassLoader();
		return loader;
	}

	private static InputStream getInputStream(String resourceName,
			String classNameWithPackage) {
		ClassLoader classLoader = getClassLoader(classNameWithPackage);
		return classLoader.getResourceAsStream(resourceName);
	}

	public static Properties getProperties(String resourceName,
			String classNameWithPackage)
			throws InvalidPropertiesFormatException, IOException {
		String absoluteResourceName = classNameWithPackage + "-" + resourceName;
		Properties properties;
		if (cacheResources == null) {
			cacheResources = new HashMap<String, Properties>();
		}
		if (cacheResources.containsKey(absoluteResourceName)) {
			properties = cacheResources.get(absoluteResourceName);
			return properties;
		} else {
			properties = new Properties();
			properties.loadFromXML(getInputStream(resourceName,
					classNameWithPackage));
			cacheResources.put(absoluteResourceName, properties);
		}
		return properties;
	}

}
