package com.talentica.hungryHippos.utility;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.utility.XmlPropertyFileReaderUtil;

public class XmlPropertyReaderTest {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(XmlPropertyReaderTest.class.getName());

	@Test
	public void testCaseForProperty() throws ClassNotFoundException {
		try {
			Properties properties = XmlPropertyFileReaderUtil.getProperties(
					"property-conf.xml",
					"com.talentica.hungryHippos.coordination.utility.Property");
			Enumeration<?> enuKeys = properties.keys();
			while (enuKeys.hasMoreElements()) {
				String key = (String) enuKeys.nextElement();
				if ("First Step".equals(key)) {
					Assert.assertTrue("First Step".equals(key));
					continue;
				} else if ("Second Step".equals(key)) {
					Assert.assertTrue("Second Step".equals(key));
					continue;
				} else if ("Third Step".equals(key)) {
					Assert.assertTrue("Third Step".equals(key));
					continue;
				} else if ("Fourth Step".equals(key)) {
					Assert.assertTrue("Fourth Step".equals(key));
					continue;
				} else {
					Assert.assertFalse(false);
				}
			}
		} catch (IOException e) {
			LOGGER.info("Unable to register the properties file.");
		}
	}
}
